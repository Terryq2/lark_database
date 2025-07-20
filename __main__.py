from requester import YKYRequester

if __name__ == "__main__":
    test = YKYRequester()
    test.get_financial_data([("C04", "month", "2025-06")])
