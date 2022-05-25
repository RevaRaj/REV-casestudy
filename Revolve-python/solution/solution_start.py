import argparse
from app import ETL


class Solution(ETL):
    def __init__(self):

        self.main()
        

    def get_params(self) -> dict:
        parser = argparse.ArgumentParser(description='DataTest')
        parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
        parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
        parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
        parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
        return vars(parser.parse_args())



    def main(self):
        params = self.get_params()

        ETL(params)

if __name__ == "__main__":
    Solution()
