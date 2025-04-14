class InsufficientFundsError(Exception):
    def __init__(self, available_amount, required_amount):
        self.available_amount = available_amount
        self.required_amount = required_amount
        message = f"Insufficient funds: Available: {available_amount}, Required: {required_amount}"
        super().__init__(message)