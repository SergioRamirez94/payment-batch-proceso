import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import uuid

from services.wallet_service import (
    get_wallets_by_ids,
    get_wallet_by_account_id,
    create_wallets,
    create_wallets_batch,
    get_integration_wallet_or_create,
    validate_funds,
    add_funds_to_wallet,
    subtract_funds_from_wallet
)

from database.models.wallets import Wallet
from database.models.currency import Currency
from database.models.accounts import Tenant
from database.models.country import Country
from sqlalchemy.exc import SQLAlchemyError
from exceptions.insufficient_funds_error import InsufficientFundsError


class TestWalletService(unittest.TestCase):

    def setUp(self):
        self.tenant = Tenant.tikin
        self.currency_id = uuid.uuid4()
        self.country_id = uuid.uuid4()
        self.wallet_id = uuid.uuid4()
        self.account_id = uuid.uuid4()
        
        self.country = Country(country_id=uuid.uuid4(), code="CO", country_name="Colombia", currency_currency_id=self.currency_id)
        self.currency = Currency(currency_id=self.currency_id, name = "COP", iso_code = "COP", country=[self.country])

        self.df = pd.DataFrame([
            {"identifier": "+3002005020", "account_id": str(uuid.uuid4()), "wallet_id": None, "amount": 100, "amount_fee": 5}
        ])

    def test_get_wallets_by_ids(self):
        mock_session = MagicMock()
        mock_wallets = [MagicMock(), MagicMock()]
        mock_session.query().filter().all.return_value = mock_wallets

        wallets = get_wallets_by_ids(mock_session, [uuid.uuid4()])
        self.assertEqual(wallets, mock_wallets)

    def test_get_wallet_by_account_id(self):
        mock_session = MagicMock()
        mock_wallet = MagicMock()
        mock_session.query().filter().first.return_value = mock_wallet

        result = get_wallet_by_account_id(mock_session, "account123", self.currency)
        self.assertEqual(result, mock_wallet)

    def test_create_wallets(self):
        df_test = pd.DataFrame([
            {"account_id": "acc1", "wallet_id": "w1"}
        ])
        wallets = create_wallets(df_test, self.currency)
        self.assertEqual(len(wallets), 1)
        self.assertIsInstance(wallets[0], Wallet)
        self.assertEqual(wallets[0].wallet_name, "COP")
        self.assertEqual(wallets[0].balance, 0.0)

    @patch("services.wallet_service.uuid.uuid4", return_value=uuid.UUID("12345678-1234-5678-1234-567812345678"))
    def test_create_wallets_batch(self, mock_uuid):
        mock_session = MagicMock()
        mock_session.commit = MagicMock()
        mock_session.rollback = MagicMock()

        df_result = create_wallets_batch(mock_session, self.df.copy(), self.currency)

        self.assertIn("wallet_id", df_result.columns)
        self.assertTrue(df_result["wallet_id"].notna().all())
        self.assertTrue(mock_session.add_all.called)
        self.assertTrue(mock_session.commit.called)

    @patch.dict("services.wallet_service.DICT_ACCOUNT", {"beu": "acc_b123"})
    def test_get_integration_wallet_or_create(self):
        mock_session = MagicMock()
        mock_wallet = MagicMock()
        mock_session.query().filter().first.return_value = mock_wallet

        wallet = get_integration_wallet_or_create(mock_session, self.tenant, self.currency)
        self.assertEqual(wallet, mock_wallet)

    @patch("services.wallet_service.uuid.uuid4", return_value=uuid.UUID("87654321-4321-6789-4321-678987654321"))
    def test_create_wallets_batch_failure(self, mock_uuid):
        mock_session = MagicMock()
        mock_session.commit.side_effect = SQLAlchemyError("db error")
        mock_session.rollback = MagicMock()

        create_wallets_batch(mock_session, self.df.copy(), self.currency)

        self.assertTrue(mock_session.rollback.called)

    def test_validate_funds_insufficient(self):
        wallet = MagicMock(balance=10)
        with self.assertRaises(InsufficientFundsError):
            validate_funds(wallet, 20)

    def test_add_and_subtract_funds_wallet(self):
        wallet = MagicMock(balance=10)
        wallet = add_funds_to_wallet(wallet, 5)
        self.assertEqual(wallet.balance, 15)
        wallet = subtract_funds_from_wallet(wallet, 3)
        self.assertEqual(wallet.balance, 12)


if __name__ == "__main__":
    unittest.main()
