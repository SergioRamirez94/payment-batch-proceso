import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import uuid

from services.account_service import create_accounts, suggest_name, create_account_name, create_accounts_batch
from database.models.accounts import Account, Tenant
from database.models.currency import Currency
from database.models.country import Country
from database.models.accounts import Account
from database.models.wallets import Wallet
import uuid

class TestAccountCreator(unittest.TestCase):

    def setUp(self):
        self.tenant = Tenant.tikin
        self.currency_id = uuid.uuid4()
        self.country_id = uuid.uuid4()
        self.wallet_id = uuid.uuid4()
        self.account_id = uuid.uuid4()
        
        self.country = Country(country_id=uuid.uuid4(), code="CO", country_name="Colombia", currency_currency_id=self.currency_id)
        self.currency = Currency(currency_id=self.currency_id, name = "COP", iso_code = "COP", country=[self.country])
        self.df = pd.DataFrame([
            {"identifier": "+3002005020", "account_id": None, "wallet_id": None, "amount": 100, "amount_fee": 10}
        ])
        

    def test_create_accounts(self):
        group = pd.DataFrame([
            {"account_id": self.account_id, "user_name": "$user1"}
        ])
        accounts = create_accounts(group, self.currency, self.tenant)
        print('entro aca')
        self.assertEqual(len(accounts), 1)
        self.assertIsInstance(accounts[0], Account)
        self.assertEqual(accounts[0].user_name, "$user1")
        self.assertEqual(accounts[0].currency_id, self.currency_id)
        self.assertEqual(accounts[0].country_country_id, self.country.country_id)
        self.assertIn("https//tikin.pro/$user1", accounts[0].business_url)

    def test_suggest_name_unique(self):
        mock_session = MagicMock()
        mock_session.query().filter().first.return_value = None
        suggested = suggest_name("$user1", mock_session, self.tenant)
        self.assertEqual(suggested, "$user1")

    def test_suggest_name_collision(self):
        mock_session = MagicMock()
        mock_session.query().filter().first.side_effect = [MagicMock(), None]
        with patch("random.randint", return_value=123):
            suggested = suggest_name("user1", mock_session, self.tenant)
            self.assertEqual(suggested, "user1123")

    @patch("services.account_service.coolname.generate_slug", return_value="user-slug")
    def test_create_account_name(self, mock_slug):
        mock_session = MagicMock()
        mock_session.query().filter().first.return_value = None
        name = create_account_name(mock_session, self.tenant)
        self.assertTrue(name.startswith("$user-slug"))

    @patch("services.account_service.create_wallets")
    @patch("services.account_service.send_notification_create_account")
    @patch("services.account_service.create_account_name")
    def test_create_accounts_batch(self, mock_name, mock_notify, mock_wallets):
        mock_name.return_value = "user_test"
        mock_wallets.return_value = [MagicMock()]
        mock_session = MagicMock()
        mock_session.commit = MagicMock()

        df_result = create_accounts_batch(mock_session, self.df.copy(), self.currency, self.tenant)

        self.assertIn("account_id", df_result.columns)
        self.assertIn("wallet_id", df_result.columns)
        self.assertTrue(mock_notify.called)


if __name__ == '__main__':
    unittest.main()
