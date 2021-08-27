import urllib3

urllib3.disable_warnings()

from influxdb import InfluxDBClient
from collections import defaultdict
import requests
import pprint
import os
import sys
import itertools
from dotenv import load_dotenv
import logging
import datetime


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

class YouTrack(object):

    def __init__(self, url, token):
        self._url = url
        self._token = token
        self._session = requests.Session()
        self._session.headers['Authorization'] = f'Bearer {self._token}'

    def getIssues(self, project_id, query, skip, top):
        fields = "summary,customFields(name,value(minutes,name)),created,resolved,idReadable"
        params = {'fields': fields, '$top': top, '$skip': skip,
                  'query': f'#{project_id} {query}'}
        with self._session.get(f'{self._url}/api/issues', params=params) as r:
            return r.json()

    def get_changes_for_issue(self, issue_id):
        fields = "field(name,presentation),timestamp,target,targetMember,added(name),removed(name)"
        params = {'fields': fields, 'categories': 'CustomFieldCategory'}
        with self._session.get(f'{self._url}/api/issues/{issue_id}/activities', params=params) as r:
            return r.json()

    def getWorkItems(self, issue_id):
        fields = "author(login),created,type(name),date,duration(minutes)"
        params = {'fields': fields,
                  'query': f'#{issue_id}'}
        with self._session.get(f'{self._url}/api/workItems', params=params) as r:
            return r.json()

    def getProjectTimeTrackingSettings(self, project):
        fields = "enabled"
        with self._session.get(f'{self._url}/api/admin/projects/{project["id"]}/timeTrackingSettings',
                               params={'fields': fields}) as r:
            return r.json()

    def getProjects(self):
        fields = "shortName,description,id"
        with self._session.get(f'{self._url}/api/admin/projects', params={'fields': fields}) as r:
            return r.json()

class YouTrackExporter(object):

    def __init__(self, yt, influx):
        self.yt = YouTrack(**yt)
        self.influx = InfluxDBClient(**influx)
        self.logger = logging.getLogger('worktime_reporter')
        self.project_blacklist = ['RRS', 'RR_INS', 'TPLABTMPL']

    def get_all_issues(self, project):
        def _gen():
            skip = 0
            while True:
                issues = self.yt.getIssues(project['shortName'],
                                           # "#RR-1234", skip, 50)
                                           'Spent time: -?', skip=skip, top=50)
                if not issues:
                    break
                skip += len(issues)
                yield issues
        return itertools.chain.from_iterable(_gen())

    def get_custom_field(self, issue, name, default=None):
        for field in issue["customFields"]:
            if name == field['name']:
                value = field['value']
                if value:
                    if isinstance(value, dict):
                        if value['$type'] == 'EnumBundleElement':
                            return value.get('name', default)
                        if value['$type'] == 'PeriodValue':
                            return value.get('minutes', default)
                    return value
        return default

    def issue_to_measurement(self, project, issue):
        self.logger.info(f'Looking into issue {issue["idReadable"]}')
        lead_time = self.get_custom_field(issue, 'Lead time')
        stream = self.get_custom_field(issue, 'Stream')
        target = self.get_custom_field(issue, 'Target', 'Core')
        _type = self.get_custom_field(issue, 'Type')
        estimation = int(self.get_custom_field(issue, 'Estimation', 0))
        tags = {
            'project': project['shortName'],
            'issue': issue["idReadable"],
            'title': issue["summary"],
            'stream': stream,
            'target': target,
            'type': _type
        }
        counter = defaultdict(lambda: 0)
        created = int(issue['created'])
        if created:
            yield {
                'measurement': 'issue',
                'tags': tags,
                'time': created,
                'fields': {
                    'Analytics': 0,
                    'Development': 0,
                    'Testing': 0,
                    'cycle_time': 0,
                    'state': 'Submitted'
                }
            }
        states = []
        last_state_update = int(created)
        last_state = 'Submitted'
        for change in self.yt.get_changes_for_issue(issue['idReadable']):
            if change['field']['name'] == 'State':
                fields = {
                    'state': change['added'][0]['name']
                }
                if estimation:
                    fields['estimation'] = estimation
                u = int(change['timestamp'])
                yield {
                    'measurement': 'issue',
                    'tags': tags,
                    'time': u,
                    'fields': fields
                }
                states.append(
                    (last_state_update, u, change['removed'][0]['name'])
                )
                last_state_update = u
                last_state = change['added'][0]['name']
        states.append((last_state_update, last_state_update + 365*24*60*60*1000, last_state))
        for work_item in self.yt.getWorkItems(issue['idReadable']):
            _tags = dict(tags)
            _tags['author'] = work_item['author']['login']
            created = int(work_item['created'])
            work_type = work_item['type']['name']
            date = work_item.get('date', created)
            duration = work_item['duration']['minutes']
            if not created:
                continue
            created_dt = datetime.datetime.fromtimestamp(float(created)/1000)
            date_dt = datetime.datetime.fromtimestamp(float(date)/1000)
            time_dt = datetime.datetime.combine(date_dt.date(), created_dt.time(), date_dt.tzinfo)
            time_ts = int(datetime.datetime.timestamp(time_dt)*1000)
            counter[work_type] += duration
            measure = {
                'measurement': 'work_item',
                'tags': _tags,
                'time': time_ts,
                'fields': {
                    work_type: duration
                }
            }
            yield measure

            cycle_time = counter['Analytics'] + counter['Development'] + counter['Testing']
            fields = {
                'Analytics': counter['Analytics'],
                'Development': counter['Development'],
                'Testing': counter['Testing'],
                'cycle_time': cycle_time,
            }
            if estimation:
                fields['estimation'] = estimation
            if lead_time:
                fields['lead_time'] = float(lead_time)
            for _l, r, s in states:
                if _l <= time_ts < r:
                    fields['state'] = s
                    break
            yield {
                'measurement': 'issue',
                'tags': tags,
                'time': time_ts,
                'fields': fields
            }

    def process_project(self, project):
        if project['shortName'] in self.project_blacklist:
            self.logger.info(f'Skipping project {project["shortName"]}, blacklisted')
            return

        ts = self.yt.getProjectTimeTrackingSettings(project)
        if not ts["enabled"]:
            self.logger.info(
                f'Skipping project {project["shortName"]}, no time tracking')
            return

        self.logger.info(f'Looking into project {project["shortName"]}')
        issues = self.get_all_issues(project)
        measurements = (self.issue_to_measurement(project, issue) for issue in issues)
        return itertools.chain.from_iterable(measurements)

    def export(self):
        def _gen():
            projects = self.yt.getProjects()
            for project in projects:
                measurements = self.process_project(project)
                if measurements:
                    yield measurements

        self.influx.drop_measurement('issue')
        self.influx.drop_measurement('work_item')

        measurements = itertools.chain.from_iterable(_gen())
        for chunk in grouper(measurements, 50):
            filtered = list(filter(lambda x: x is not None, chunk))
            # pprint.pprint(filtered)
            self.influx.write_points(filtered, time_precision='ms')


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('main')
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    try:
        load_dotenv()
        exporter = YouTrackExporter(
            yt={
                'url': os.getenv('YT_URL'),
                'token': os.getenv('YT_TOKEN')
            },
            influx={
                'host': os.getenv('INFLUX_HOST'),
                'port': int(os.getenv('INFLUX_PORT')),
                'username': os.getenv('INFLUX_USERNAME'),
                'password': os.getenv('INFLUX_PASSWORD'),
                'database': os.getenv('INFLUX_DATABASE'),
                'ssl': True,
            })
        exporter.export()
    except: # noqa
        logger.exception("Unhandled exception")
        return -1
    return 0


sys.exit(main())
