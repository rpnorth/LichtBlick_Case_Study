import pandas as pd
import os

path = "./src_data/"
file_list = os.listdir(path)


class LoadDataTables:
    """
    Loading, cleaning the three data tables for one download date
    """

    def __init__(self, download_date):
        """
        download_date (str): date the data was downloaded as csv, in filename

        """

        self.download_date_str = download_date

    def get_files_by_date(self):
        """
        for a given download date, get the relevant csv filenames that contain
        the data (products, prices, contracts)

        """
        self.filenames = [k for k in file_list if self.download_date_str in k]

    def csv_to_df(self, filename):
        """
        read a csv file for a given date,
        add filename date as new column

        Args:
            filename (str) : name of file to read
        """
        df = pd.read_csv(path + filename, delimiter=";")
        df["download_date"] = pd.to_datetime(self.download_date_str)
        return df

    def load_data_tables(self):
        """
        based on filenames, load the three tables for a given download date
        """
        for file in self.filenames:
            if "contracts" in file:
                self.df_contracts = self.csv_to_df(file)
            if "products" in file:
                self.df_products = self.csv_to_df(file)
            if "prices" in file:
                self.df_prices = self.csv_to_df(file)

    def fix_dates(self):
        """
        make sure all dates are datetime format, add download date from 
        filename as column
        """
        # fix dates to correct format
        self.df_contracts[
            [
                "createdat",
                "startdate",
                "enddate",
                "fillingdatecancellation",
                "modificationdate",
            ]
        ] = self.df_contracts[
            [
                "createdat",
                "startdate",
                "enddate",
                "fillingdatecancellation",
                "modificationdate",
            ]
        ].apply(
            pd.to_datetime, errors="coerce"
        )
        self.df_products[["modificationdate"]] = self.df_products[
            ["modificationdate"]
        ].apply(pd.to_datetime, errors="coerce")
        self.df_prices[["valid_from", "valid_until", "modificationdate"]] = (
            self.df_prices[["valid_from", "valid_until", "modificationdate"]].apply(
                pd.to_datetime, errors="coerce"
            )
        )

    def fix_fields(self):
        """
        change names and drop columns for consistency
        """
        # for consistency across tables
        if "id" in self.df_products.columns:
            self.df_products = self.df_products.rename(columns={"id": "productid"})
        # to avoid confusion, remove price id as it isn't used
        if "id" in self.df_prices.columns:
            self.df_prices = self.df_prices.drop(columns=["id"])

    def remove_outliers(self):
        """
        EDA found a strange value, since no info available, remove to be safe
        """
        self.df_contracts = self.df_contracts[self.df_contracts.usage < 1e8]

    def remove_duplicate_contracts(self):
        """
        in case there is duplicated information, remove
        """
        print(self.df_contracts.shape)
        self.df_contracts = self.df_contracts.drop_duplicates()
        print(self.df_contracts.shape)

    def remove_inactive(self):
        """
        drop inactive customers - focus on active customers at time data was 
        downloaded
        """
        self.df_contracts = self.df_contracts[self.df_contracts.enddate.isnull()]
        print(self.df_contracts.shape)

    def remove_product_2000(self):
        """
        drop any customer with productid = 2000, no price available
        """
        self.df_contracts = self.df_contracts.query("productid != 2000")

    def run_load_data(self):
        self.get_files_by_date()
        self.load_data_tables()
        self.fix_dates()
        self.fix_fields()
        self.remove_outliers()
        self.remove_duplicate_contracts()
        self.remove_inactive()
        self.remove_product_2000()


class CreateRevenueTable(LoadDataTables):
    """
    Calculate revenue for each set of data tables (separated by Download Date)
    """

    def __init__(self, period_start_date, download_date):
        """
        period_start_date (str): start of period of interest, over which 
        revenue is calculated
        download_date (str): date the data was downloaded as csv, in filename

        """
        self.period_start_date = pd.to_datetime(period_start_date)
        self.download_date = pd.to_datetime(download_date)
        self.download_date_str = download_date

    def join_price_product(self):
        """
        get the product name for each product price
        """
        prices_columns = [
            "productid",
            "productcomponent",
            "price",
            "unit",
            "valid_from",
            "valid_until",
            "download_date",
        ]
        products_columns = ["productid", "productname"]
        self.df_price_product = self.df_prices[prices_columns].merge(
            self.df_products[products_columns],
            left_on="productid",
            right_on="productid",
        )

    def join_contract_price(self):
        """
        add pricing/product info to contract table, reduce to only columns 
        of interest
        """
        contracts_columns = [
            "id",
            "usage",
            "startdate",
            "enddate",
            "status",
            "productid",
            "modificationdate",
            "createdat",
            "download_date",
        ]
        self.df = self.df_contracts[contracts_columns].merge(
            self.df_price_product, left_on="productid", right_on="productid"
        )

    def drop_contracts(self):
        """
        drop any contracts that don't fit between period_start_date and 
        download_date will also drop if dates are before price info is
        available
        """
        self.df = self.df.drop(
            self.df[
                (self.df.enddate < self.period_start_date)
                | (self.df.startdate > self.download_date)
                | (self.df.enddate < self.df.valid_from)
                | (self.df.startdate > self.df.valid_until)
            ].index
        )

    def fix_end_dates(self):
        """
        fill any nan date values with
        """
        # set enddate to download_date date if empty
        self.df["enddate"] = self.df["enddate"].fillna(value=self.download_date)

    def get_price_dates(self):
        """
        determine dates that each price is applicable, save as new columns
        want status as of data download, so setting all end dates to
        download_date or less and start dates to period_start_date or later
        """
        # first assumed date_from, date_to are startdate, enddate
        self.df["date_from"] = self.df["startdate"]
        self.df["date_to"] = self.df["enddate"]
        # check that date_from and date_to are not outside period_start_date 
        # and donwload_date
        self.df.loc[self.df.date_to > self.download_date, "date_to"] = (
            self.download_date
        )
        self.df.loc[self.df.date_from < self.period_start_date, "date_from"] = (
            self.period_start_date
        )

        # get start and end date of each price, depending on supply dates 
        # and price dates
        self.df["date_from"] = self.df[["startdate", "valid_from"]].max(axis=1)
        self.df["date_to"] = self.df[["enddate", "valid_until"]].min(axis=1)

        # length of period this price is applicable
        self.df["price_period_days"] = (self.df.date_to - self.df.date_from).dt.days

    def calc_working_cost(self):
        """
        calculate usage*working_price for specified time period

        """
        self.df["working_cost_yr"] = (
            self.df.usage * self.df.price / 100
        )  # kwh/yr x ct/kwh /100 = €/yr
        self.df["working_cost_day"] = self.df.working_cost_yr / (
            365
        )  # usage €/day (approx)
        self.df["working_cost_period"] = (
            self.df.working_cost_day * self.df.price_period_days
        )  # €/period

        self.df["usage_day"] = self.df.usage / 365  # kwh/day (approx)
        self.df["usage_period"] = (
            self.df.usage_day * self.df.price_period_days
        )  # kwh/period
        # dataframe of working_cost_period only
        self.df_working_cost = self.df.query('productcomponent == "workingprice"')[
            [
                "id",
                "usage_period",
                "working_cost_period",
                "date_from",
                "date_to",
                "download_date_x",
                "productname",
                "createdat",
            ]
        ]

    def calc_base_cost(self):
        """
        calculate base_price for specified time period

        """
        self.df["base_cost_day"] = self.df.price / 365  # €/day (approx)
        self.df["base_cost_period"] = (
            self.df.base_cost_day * self.df.price_period_days
        )  # €/period
        # dataframe of base_cost_period only
        self.df_base_cost = self.df.query('productcomponent == "baseprice"')[
            ["id", "base_cost_period", "date_from", "date_to"]
        ]

    def calc_revenue(self):
        """
        calculate revenue by table row
        """
        self.df_w_revenue = pd.merge_asof(
            self.df_working_cost.sort_values("date_from"),
            self.df_base_cost.sort_values("date_from"),
            on="date_from",
            by="id",
        )

        self.df_w_revenue["revenue_euro"] = self.df_w_revenue[
            ["base_cost_period", "working_cost_period"]
        ].sum(axis=1)

    def create_output_table(self):
        """
        get revenue per customer at time data was downloaded and output 
        table in requested format:
        "Verbrauch (consumption) and Umsatz (revenue) attached to the 
        attributes Anlagedatum (creation date) and Produkt (product)"
        """
        self.df_output = self.df_w_revenue.groupby(
            ["id", "download_date_x", "productname", "createdat"]
        )[["revenue_euro", "usage_period"]].sum()
        self.df_output = self.df_output.reset_index()
        self.df_output = self.df_output.rename(
            columns={
                "id": "customer_id",
                "usage_period": "consumption",
                "revenue_euro": "revenue",
                "createdat": "creation_date",
                "productname": "product",
                "download_date_x": "download_date",
            }
        )

    def get_statistics(self):
        """
        calculate various statistics related to stakeholder request
        """
        # number of active contracts
        self.number_contracts = self.df_output.customer_id.count()
        # total revenue
        self.total_revenue = self.df_output.revenue.sum()
        # average revenue per contract
        self.av_revenue_per_contract = self.total_revenue / self.number_contracts

    def run_create_revenue_table(self):
        self.join_price_product()
        self.join_contract_price()
        self.drop_contracts()
        self.fix_end_dates()
        self.get_price_dates()
        self.calc_working_cost()
        self.calc_base_cost()
        self.calc_revenue()
        self.create_output_table()
        self.get_statistics()
