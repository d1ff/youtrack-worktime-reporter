from youtrack.connection import Connection as YouTrack
from influxdb import InfluxDBClient
from collections import defaultdict
import os
import sys
import itertools
from dotenv import load_dotenv
import logging
import datetime


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


class YouTrackExporter(object):

    def __init__(self, yt, influx):
        self.yt = YouTrack(**yt)
        self.influx = InfluxDBClient(**influx)
        self.logger = logging.getLogger('worktime_reporter')
        self.project_blacklist = ['RRS', 'RR_INS']

    def get_all_issues(self, project):
        def _gen():
            skip = 0
            while True:
                issues = self.yt.getIssues(project.id,
                                           'Spent time: -?', skip, 50)
                if not issues:
                    break
                skip += len(issues)
                yield issues
        return itertools.chain.from_iterable(_gen())

    def issue_to_measurement(self, project, issue):
        self.logger.info(f'Looking into issue {issue.id}')
        lead_time = getattr(issue, 'Lead time', None)
        stream = getattr(issue, 'Stream', None)
        target = getattr(issue, 'Target', 'Core')
        _type = getattr(issue, 'Type', None)
        resolved = getattr(issue, 'resolved', None)
        estimation = getattr(issue, 'Estimation', None)
        tags = {
            'project': project.id,
            'issue': issue.id,
            'stream': stream,
            'target': target,
            'type': _type
        }
        counter = defaultdict(lambda: 0)
        for work_item in self.yt.getWorkItems(issue.id):
            _tags = dict(tags)
            _tags['author'] = getattr(work_item, 'authorLogin', None)
            created = getattr(work_item, 'created', None)
            work_type = getattr(work_item, 'worktype', None)
            date = getattr(work_item, 'date', created)
            duration = int(work_item.duration)
            if not created:
                continue
            created_dt = datetime.datetime.fromtimestamp(float(created)/1000)
            date_dt = datetime.datetime.fromtimestamp(float(date)/1000)
            time_dt = datetime.datetime.combine(date_dt.date(), created_dt.time(), date_dt.tzinfo)
            time_ts = int(datetime.datetime.timestamp(time_dt)*1000)
            counter[work_type] += duration
            yield {
                'measurement': 'work_item',
                'tags': _tags,
                'time': time_ts,
                'fields': {
                    work_type: duration
                }
            }

        cycle_time = counter['Analytics'] + counter['Development'] + counter['Testing']
        created = getattr(issue, 'created', None)
        if estimation:
            yield {
                'measurement': 'issue',
                'tags': tags,
                'time': int(created),
                'fields': {
                    'estimation': int(estimation)
                }
            }
        if lead_time and resolved:
            yield {
                'measurement': 'issue',
                'tags': tags,
                'time': int(resolved),
                'fields': {
                    'lead_time': float(lead_time),
                    'cycle_time': cycle_time,
                }
            }

    def process_project(self, project):
        if project.id in self.project_blacklist:
            self.logger.info(f'Skipping project {project.id}, blacklisted')
            return

        ts = self.yt.getProjectTimeTrackingSettings(project['id'])
        if not ts.Enabled:
            self.logger.info(
                f'Skipping project {project.id}, no time tracking')
            return

        self.logger.info(f'Looking into project {project.id}')
        issues = self.get_all_issues(project)
        measurements = (self.issue_to_measurement(project, issue) for issue in issues)
        return itertools.chain.from_iterable(measurements)

    def export(self):
        def _gen():
            projects = self.yt.getProjects()
            for project_id in projects:
                project = self.yt.getProject(project_id)
                measurements = self.process_project(project)
                if measurements:
                    yield measurements

        self.influx.drop_measurement('issue')
        self.influx.drop_measurement('work_item')

        measurements = itertools.chain.from_iterable(_gen())
        for chunk in grouper(measurements, 50):
            filtered = filter(lambda x: x is not None, chunk)
            self.influx.write_points(filtered, time_precision='ms')


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('main')
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
