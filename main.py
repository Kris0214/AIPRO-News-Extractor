from src.News  import main_news
from src.Advisory_reports import main_adreports
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AIPRO Processing System")
    parser.add_argument(
        "--mode",
        choices=["news", "reports"],
        required=True,
        help="Mode to run the processing system: 'news' for News Processing, 'reports' for Advisory Reports Processing"
    )
    args = parser.parse_args()

    if args.mode == "news":
        main_news.main()
    elif args.mode == "reports":
        main_adreports.main()

