import argparse
import src.tim_etl as tim_etl

parser = argparse.ArgumentParser(prog='tim_etl',
                                 description='ETL Helper Updating Postgres with Snowflake')

parser.add_argument("command", type=str,
                    help="commands run,")
parser.add_argument("-optional", action="store_true",
                    help="something optional")

args = parser.parse_args()


def main():
    command = args.command
    if command == 'run':
        etl = tim_etl.ETL_Session()
        etl.create_script()
        print('\n\nAre you sure?? [Press y or yes to Continue]')
        answer = input()
        if answer in ('yes', 'y'):
            # etl.run()
            print('ho')
        else:
            print('Goodbye')
    elif command == 'test':
        etl = tim_etl.ETL_Session()
        etl.create_script()
    else:
        print('command not found')


main()
