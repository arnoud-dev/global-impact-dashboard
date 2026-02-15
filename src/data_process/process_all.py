from src.data_process.worldbank import run as run_worldbank
from src.data_process.metadata import run as run_metadata
from src.data_process.emissions import run as run_emissions

def main():
    run_metadata()
    run_worldbank()
    run_emissions()

if __name__ == "__main__":
    main()
