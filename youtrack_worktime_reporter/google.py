import pickle
import datetime
import os.path
from dotenv import load_dotenv
from influxdb import InfluxDBClient
import logging
from dateutil.parser import parse
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from .utils import grouper


SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def get_credentials():
    TOKEN_PATH = os.getenv('GOOGLE_TOKEN_PATH')
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDS_PATH')

    creds = None
    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'wb') as token:
            pickle.dump(creds, token)

    return creds


def process(spreadsheet_id):
    creds = get_credentials()

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    result = sheet.get(
        spreadsheetId=spreadsheet_id).execute()
    values = sheet.values()

    for sheet in result['sheets']:
        author = sheet['properties']['title']
        data_range = f'{author}!A1:K500'
        vals = values.get(
            spreadsheetId=spreadsheet_id, range=data_range).execute()
        vals = vals['values']
        header, vals = vals[0], vals[1:]
        if 'План на день' not in header:
            continue
        print(author)
        cfo_ind = set()
        for i, col in enumerate(header):
            if 'ЦФО' in col:
                cfo_ind.add(i)
        for row in vals:
            if not row:
                continue
            _date = row[0]
            try:
                _date = parse(row[0], dayfirst=True, yearfirst=False)
                _date += datetime.timedelta(hours=12)
            except: # noqa
                continue

            cfos = {}

            for cfo in cfo_ind:
                try:
                    val = row[cfo]
                except IndexError:
                    continue

                if not val:
                    continue

                try:
                    float_val = float(val.replace(',', '.'))
                    if not float_val:
                        continue
                    yield {
                        "measurement": "google",
                        "tags": {
                            "author": author,
                            "cfo": header[cfo]
                        },
                        "time": int(datetime.datetime.timestamp(_date) * 1000),
                        "fields": {
                            "value": float_val
                        }
                    }
                    yield {
                        "measurement": "google_detailed",
                        "tags": {
                            "author": author,
                        },
                        "time": int(datetime.datetime.timestamp(_date) * 1000),
                        "fields": {
                            header[cfo]: float_val
                        }
                    }

                except ValueError:
                    continue


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('main')
    try:
        load_dotenv()
        influx_args = {
            'host': os.getenv('INFLUX_HOST'),
            'port': int(os.getenv('INFLUX_PORT')),
            'username': os.getenv('INFLUX_USERNAME'),
            'password': os.getenv('INFLUX_PASSWORD'),
            'database': os.getenv('INFLUX_DATABASE'),
            'ssl': True,
        }
        influx = InfluxDBClient(**influx_args)
        metrics = process(spreadsheet_id=os.getenv('GOOGLE_SPREADSHEET_ID'))
        #influx.drop_measurement('google')
        for chunk in grouper(metrics, 50, fillvalue=None):
            filtered = filter(lambda x: x is not None, chunk)
            influx.write_points(filtered, time_precision='ms')
    except: # noqa
        logger.exception("Unhandled exception")
        return -1
    return 0


if __name__ == '__main__':
    sys.exit(main())
