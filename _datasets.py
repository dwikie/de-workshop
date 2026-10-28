import kagglehub
from kagglehub import KaggleDatasetAdapter

# Set the path to the file you'd like to load
file_path = [
    "cards_data.csv",
    "transactions_data.csv",
    "users_data.csv"
]

for file in file_path:
    # Load the latest version
    df = kagglehub.load_dataset(
    KaggleDatasetAdapter.PANDAS,
    "computingvictor/transactions-fraud-datasets",
    file,
    # Provide any additional arguments like 
    # sql_query or pandas_kwargs. See the 
    # documenation for more information:
    # https://github.com/Kaggle/kagglehub/blob/main/README.md#kaggledatasetadapterpandas
    )

    print("First 5 records:", df.head())
    df.to_csv(f"datasets/{file}", index=False)