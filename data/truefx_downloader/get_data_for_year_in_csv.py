import errno
import sys, getopt
from truefx import manager


def print_usage():
    print("Usage: python get_data_for_year_in_csv.py -u <truefxUsername> -p <truefxPassword>" +
          "-f <folder> -y <year> -s <symbol>")


def main(argv):
    try:

        truefx_username = None
        truefx_password = None
        destination_folder = None
        year = None
        symbol = None

        opts, args = getopt.getopt(argv, "hu:p:f:y:s:", ["username=", "password=", "folder=", "year=", "symbol="])

        for opt, arg in opts:

            if opt == "-h":
                print_usage()
                exit()
            elif opt in ("-u", "--username"):
                truefx_username = arg
            elif opt in ("-p", "--password"):
                truefx_password = arg
            elif opt in ("-f", "--folder"):
                destination_folder = arg
            elif opt in ("-y", "--year"):
                year = arg
            elif opt in ("-s", "--symbol"):
                symbol = arg

        if truefx_password is None or truefx_password is None or destination_folder is None \
                or year is None or symbol is None:
            print_usage()
            exit(2)

        truefxManager = manager.Manager()

        if truefxManager.login_to_true_fx(truefx_username, truefx_password):
            print("Successfully logged to TrueFx")
            truefxManager.download_and_merge_to_one_file(year, symbol, destination_folder + '\\')
        else:
            print("Can't login to TrueFx")

    except getopt.GetoptError:
        print_usage()
        sys.exit(2)

    except OSError as e:
        if e.errno == errno.EEXIST:
            print(e.strerror)
        else:
            raise e


if __name__ == "__main__":
    main(sys.argv[1:])
