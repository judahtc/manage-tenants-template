import numpy as np
import pandas as pd

from application.modeling import helper


def cal_depreciation_and_nbv(
    depreciation_rate: float, remaining_useful_life: int, nbv: float
) -> (np.ndarray, np.ndarray):
    net_book_value = (
        np.power((1 - depreciation_rate), range(1, remaining_useful_life + 1)) * nbv
    )
    depreciation = -np.diff(
        np.power((1 - depreciation_rate), range(0, remaining_useful_life + 1)) * nbv
    )
    return net_book_value, depreciation


def create_depreciation_and_nbv_series_index(
    details_of_assets, remaining_useful_life, asset, new_assets: bool, valuation_date
):
    if new_assets:
        purchase_date = details_of_assets.loc[
            details_of_assets.asset == asset, "purchase_date"
        ].values[0]

        purchase_date = helper.convert_to_datetime(purchase_date)
        index = pd.period_range(
            purchase_date, periods=remaining_useful_life, freq="M"
        ).strftime("%b-%Y")
    else:
        index = pd.period_range(
            valuation_date, periods=remaining_useful_life, freq="M"
        ).strftime("%b-%Y")
    return index


def calculate_reducing_balance_depreciation(
    details_of_assets: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
    new_assets: bool = False,
):
    depreciations = []
    nbvs = []
    df_index = helper.generate_columns(valuation_date, months_to_forecast)

    if details_of_assets.empty:
        return {"nbvs": pd.DataFrame(), "depreciations": pd.DataFrame()}

    for asset in details_of_assets.asset:
        nbv = details_of_assets.loc[
            details_of_assets.asset == asset, "net_book_value"
        ].values[0]
        remaining_useful_life = details_of_assets.loc[
            details_of_assets.asset == asset, "remaining_useful_life"
        ].values[0]
        depreciation_rate = details_of_assets.loc[
            details_of_assets.asset == asset, "depreciation"
        ].values[0]

        index = create_depreciation_and_nbv_series_index(
            details_of_assets=details_of_assets,
            remaining_useful_life=remaining_useful_life,
            asset=asset,
            new_assets=new_assets,
            valuation_date=valuation_date,
        )

        net_book_value, depreciation = cal_depreciation_and_nbv(
            depreciation_rate=depreciation_rate,
            remaining_useful_life=remaining_useful_life,
            nbv=nbv,
        )

        net_book_value = pd.Series(net_book_value, index=index, name=asset)
        depreciation = pd.Series(depreciation, index=index, name=asset)
        depreciations.append(depreciation)
        nbvs.append(net_book_value)

    return {
        "nbvs": pd.concat(nbvs, axis=1).reindex(df_index).fillna(0),
        "depreciations": pd.concat(depreciations, axis=1).reindex(df_index).fillna(0),
    }


def calculate_straight_line_depreciation(
    details_of_assets: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
    new_assets: bool = False,
):
    depreciations = []
    nbvs = []
    df_index = helper.generate_columns(valuation_date, months_to_forecast)

    if details_of_assets.empty:
        return {"nbvs": pd.DataFrame(), "depreciations": pd.DataFrame()}

    for asset in details_of_assets.asset:
        cost = details_of_assets.loc[details_of_assets.asset == asset, "cost"].iat[0]
        depreciation_rate = details_of_assets.loc[
            details_of_assets.asset == asset, "depreciation"
        ].iat[0]
        remaining_useful_life = details_of_assets.loc[
            details_of_assets.asset == asset, "remaining_useful_life"
        ].iat[0]

        net_book_value = details_of_assets.loc[
            details_of_assets.asset == asset, "net_book_value"
        ].iat[0]

        monthly_depreciation = cost * depreciation_rate

        index = create_depreciation_and_nbv_series_index(
            details_of_assets=details_of_assets,
            remaining_useful_life=remaining_useful_life,
            asset=asset,
            new_assets=new_assets,
            valuation_date=valuation_date,
        )

        depreciation = pd.Series(
            np.repeat(monthly_depreciation, remaining_useful_life),
            index=index,
            name=asset,
        )

        net_book_value = -depreciation.cumsum() + net_book_value

        depreciations.append(depreciation)
        nbvs.append(net_book_value)

    return {
        "nbvs": pd.concat(nbvs, axis=1).reindex(df_index).fillna(0),
        "depreciations": pd.concat(depreciations, axis=1).reindex(df_index).fillna(0),
    }


def calculate_depreciations_and_nbvs(
    details_of_new_assets: pd.DataFrame,
    details_of_existing_assets: pd.DataFrame,
    valuation_date: str,
    months_to_forecast: int,
):
    details_of_existing_assets_reducing_balance = details_of_existing_assets.loc[
        details_of_existing_assets.method == "reducing_balance"
    ]

    details_of_existing_assets_straight_line = details_of_existing_assets.loc[
        details_of_existing_assets.method == "straight_line"
    ]

    details_of_new_assets_reducing_balance = details_of_new_assets.loc[
        details_of_new_assets.method == "reducing_balance"
    ]

    details_of_new_assets_straight_line = details_of_new_assets.loc[
        details_of_new_assets.method == "straight_line"
    ]

    depreciation_and_nbv_of_existing_assets_reducing_balance = (
        calculate_reducing_balance_depreciation(
            details_of_assets=details_of_existing_assets_reducing_balance,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
        )
    )

    depreciation_and_nbv_of_existing_assets_straight_line = (
        calculate_straight_line_depreciation(
            details_of_assets=details_of_existing_assets_straight_line,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
        )
    )

    depreciation_and_nbv_of_new_assets_reducing_balance = (
        calculate_reducing_balance_depreciation(
            details_of_assets=details_of_new_assets_reducing_balance,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
            new_assets=True,
        )
    )

    depreciation_and_nbv_of_new_assets_straight_line = (
        calculate_straight_line_depreciation(
            details_of_assets=details_of_new_assets_straight_line,
            months_to_forecast=months_to_forecast,
            valuation_date=valuation_date,
            new_assets=True,
        )
    )

    nbvs_for_assets = pd.concat(
        [
            depreciation_and_nbv_of_new_assets_reducing_balance["nbvs"],
            depreciation_and_nbv_of_new_assets_straight_line["nbvs"],
            depreciation_and_nbv_of_existing_assets_reducing_balance["nbvs"],
            depreciation_and_nbv_of_existing_assets_straight_line["nbvs"],
        ],
        axis=1,
    )

    nbvs_for_assets = nbvs_for_assets.assign(total=nbvs_for_assets.sum(axis=1))

    depreciation_of_assets = pd.concat(
        [
            depreciation_and_nbv_of_new_assets_reducing_balance["depreciations"],
            depreciation_and_nbv_of_new_assets_straight_line["depreciations"],
            depreciation_and_nbv_of_existing_assets_reducing_balance["depreciations"],
            depreciation_and_nbv_of_existing_assets_straight_line["depreciations"],
        ],
        axis=1,
    )

    depreciation_of_assets = depreciation_of_assets.assign(
        total=depreciation_of_assets.sum(axis=1)
    )

    depreciation_of_assets = depreciation_of_assets.groupby(axis=1, level=0).sum()
    nbvs_for_assets = nbvs_for_assets.groupby(axis=1, level=0).sum()

    return {"nbvs": nbvs_for_assets, "dpns": depreciation_of_assets}
