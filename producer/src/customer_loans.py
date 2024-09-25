class customer_loans:
    def __init__(self, customer_ID: int) -> None:
        self.customer_ID = customer_ID
        self.loans = {}
        self.minition = {}