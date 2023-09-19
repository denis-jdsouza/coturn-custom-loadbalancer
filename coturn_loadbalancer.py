"""
Coturn custom loadbalancer
"""

from argparse import ArgumentParser, FileType
import asyncio
import logging
from random import choice
from statistics import mean
from time import sleep, time
from threading import Thread
from flask import Flask, jsonify
import requests
from waitress import serve
from yaml import safe_load

turn_server = None
turn_data = None

# load config file
parser = ArgumentParser(description="Coturn Loadbalancer Service")
parser.add_argument("--config-file", nargs="?", help="Configuration file path",
                    default="./coturn_loadbalancer.yaml", type=FileType("r"),)
args = parser.parse_args()
config = safe_load(args.config_file.read())

turn = config['turn']
webApi = config['webApi']

# enable logging
logLevel = config.get("logLevel", "INFO").upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)s (%(threadName)s:%(name)s) %(message)s', level=logLevel)


def web_api():
    """ Web API function """
    app = Flask(__name__)

    @app.route('/coturn', methods=['GET'])
    def get_turn_server():
        return jsonify({'turn_data': turn_data, 'turn_server': turn_server})

    @app.route('/p/health_check', methods=['GET'])
    def get_api_health():
        if turn_server:
            resp = jsonify(health="healthy")
            resp.status_code = 200
        else:
            resp = jsonify(health="unhealthy")
            resp.status_code = 500
        return resp
    return app


class HealthCheck(Thread):
    """ Class for performing health-checks on endpoints """

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.name = 'HealthCheck'
        self.healthcheck = config['healthCheck']

    @staticmethod
    def check_current_server(server):
        """ Function to check if the current TURN server has become unhealthy """
        if (turn_server
            and
            len([0 for v in turn_data.values() if v['healthy'] is False]) in range(1,len(turn_data))
            and
            turn_server not in [srv for srv, data in turn_data.items() if data['healthy'] is True]):
            logging.warning('Current TURN server %s changed status to Unhealthy', server)
            Fallback().random_healthy_server()

    def process_health_check(self, server, healthcheck_passed):
        """ Function to process health of endpoints """
        global turn_data
        # update 'failed_checks' value
        if turn_data[server]['healthy'] is None:
            logging.debug('Initial health-check..')
            if healthcheck_passed:
                turn_data[server]['failed_checks'] = 0
            else:
                turn_data[server]['failed_checks'] = turn_data[server]['success_th']
        else:
            if healthcheck_passed:
                if turn_data[server]['failed_checks'] == turn_data[server]['initial']:
                    pass
                else:
                    # check if health-checks passed are 'consecutive'
                    if (turn_data[server]['healthy'] is True
                        and turn_data[server]['failed_checks'] > turn_data[server]['initial']):
                        turn_data[server]['failed_checks'] = turn_data[server]['initial']
                    else:
                        turn_data[server]['failed_checks'] -= 1
            else:
                if turn_data[server]['failed_checks'] == turn_data[server]['success_th']:
                    pass
                else:
                    # check if health-checks failed are 'consecutive'
                    if (turn_data[server]['healthy'] is False
                        and turn_data[server]['failed_checks'] < turn_data[server]['success_th']):
                        turn_data[server]['failed_checks'] = turn_data[server]['success_th']
                    else:
                        turn_data[server]['failed_checks'] += 1
        # update 'healthy' value
        if turn_data[server]['failed_checks'] == 0:
            if not turn_data[server]['healthy'] is True:
                turn_data[server]['healthy'] = True
                logging.info('Server: %s changed status to Healthy', server)
        elif turn_data[server]['failed_checks'] == turn_data[server]['success_th']:
            if not turn_data[server]['healthy'] is False:
                turn_data[server]['healthy'] = False
                logging.warning('Server: %s changed status to Unhealthy', server)
        self.check_current_server(server)

    async def _get_health_tcp(self, server):
        try:
            _reader, writer = await asyncio.wait_for(asyncio.open_connection(server,
                                                    self.healthcheck['port']),
                                                    timeout=self.healthcheck['timeoutSeconds'])
            writer.close()
            await writer.wait_closed()
        except Exception as error:
            logging.warning('Unable to connect %s, error: %s', server, error.__class__)
            self.process_health_check(server, healthcheck_passed=False)
        else:
            logging.debug('Successfully connected %s', server)
            self.process_health_check(server, healthcheck_passed=True)

    async def main_tcp(self):
        """ Function to probe the health of endpoints (Async) """
        address = tuple(turn['addressMapping'].values())
        await asyncio.gather(*[self._get_health_tcp(server) for server in address])

    def run(self):
        """ Main Health-Check function """
        global turn_data
        success_th = self.healthcheck['successThreshold']
        turn_data = {server: dict(initial=0,
                                  success_th=success_th,
                                  failed_checks=None,
                                  healthy=None) for server in turn['addressMapping'].values()}
        logging.debug('initializing data structure: %s', turn_data)

        while True:
            start_time = time()
            asyncio.run(self.main_tcp())
            logging.debug('Current data structure: %s', turn_data)
            logging.debug('Current turn_server: %s', turn_server)
            end_time = time()
            sleep_time = max(0, self.healthcheck['intervalSeconds'] - (end_time - start_time))
            logging.debug('Sleeping for: %ss', round(sleep_time, 2))
            sleep(sleep_time)


class LoadBalancer(Thread):
    """ Class for loadbalancing across endpoints """

    def __init__(self):
        Thread.__init__(self)
        self.daemon = True
        self.name = 'LoadBalancer'
        self.loadbalancer = config['loadBalancer']
        self.step_duration = 60

    def get_prometheus_metrics(self, healthy_servers):
        """ Function to query Prometheus for metrics """
        start_time = round(time() - (self.loadbalancer['durationMinutes'] * 60))
        end_time = round(time())
        endpoint = self.loadbalancer['prometheus']['endpoint']
        port = self.loadbalancer['prometheus']['port']
        url = f"{endpoint}:{port}/api/v1/query_range"
        server_list = '|'.join(healthy_servers)
        placeholder_count = self.loadbalancer['prometheus']['query'].count('%s')
        query = self.loadbalancer['prometheus']['query'] % tuple(
            [server_list for i in range(placeholder_count)])
        logging.debug('Prometheus query: %s', query)
        query_parameters = {'query': query, 'start': start_time,
                            'end': end_time, 'step': self.step_duration}
        try:
            response = requests.get(
                url=url, params=query_parameters, timeout=self.loadbalancer['timeoutSeconds'])
        except Exception as error:
            return error
        else:
            if not response.ok:
                return f'{response}'
            result = response.json()['data']['result']
            logging.debug('Prometheus query result: %s', result)
            return result

    def process_metrics(self, query_result):
        """ Function to process Prometheus metrics """
        values_dict = {}
        for server_metric in query_result:
            lines_list = []
            metric_label = None
            for label, value in server_metric['metric'].items():
                if value in turn['addressMapping'].values():
                    metric_label = label
                    break
            if not metric_label:
                raise Exception("Unable to identify servers from metrics lables,"
                                " check Prometehus query and addressMapping", )
            for value in server_metric['values']:
                lines_list.append(float(value[1]))
            if len(lines_list) < self.loadbalancer['durationMinutes']:
                logging.info('Not enough data points, ignoring server: %s, mertic count: %s',
                             server_metric["metric"][metric_label], len(lines_list))
            else:
                values_dict[server_metric['metric'][metric_label]] = lines_list
        return values_dict

    @staticmethod
    def select_server(metrics):
        """ Function to select the best TURN server based on metrics """
        mean_metrics = {server: mean(data_points)
                        for server, data_points in metrics.items()}
        min_value = min(mean_metrics.values())
        all_matches = {server: mean_data for server,
                       mean_data in mean_metrics.items() if mean_data == min_value}
        logging.debug('Best TURN servers: %s', all_matches)
        final_server = choice(list(all_matches))
        logging.debug('Final TURN server: %s', final_server)
        global turn_server
        turn_server = final_server

    def run(self):
        """ Main Load-Balancer function """
        while True:
            start_time = time()
            while True:
                if (turn_data is None
                    or [True for data in turn_data.values() if data['healthy'] is None]):
                    logging.debug('Waiting for initial Health-check to complete..')
                    sleep(2)
                else:
                    break
            if self.loadbalancer['algorithm'] == 'random':
                Fallback().random_healthy_server()
            else:
                healthy_servers = [server for server, data in turn_data.items() if data['healthy']]
                if healthy_servers:
                    if len(healthy_servers) == 1:
                        logging.debug('Only 1 healthy server exists, skipping querying for metrics')
                        global turn_server
                        turn_server = ''.join(healthy_servers)
                    else:
                        query_result = self.get_prometheus_metrics(healthy_servers)
                        if not isinstance(query_result, list):
                            logging.warning('Error quering Prometheus, using fallback:'
                                            ' random_healthy_server, error: %s', query_result)
                            Fallback().random_healthy_server()
                        elif query_result != []:
                            metrics = self.process_metrics(query_result)
                            if metrics:
                                self.select_server(metrics)
                            else:
                                logging.warning(
                                    'Not enough metric, using fallback: random_healthy_server')
                                Fallback().random_healthy_server()
                        else:
                            logging.warning('No hits for query: %s, using fallback:'
                                            ' random_healthy_server', query_result)
                            Fallback().random_healthy_server()
                else:
                    logging.warning('All servers are unhealthy, using fallback: random_server')
                    Fallback().random_server()
            end_time = time()
            sleep_time = max(0, (self.loadbalancer['intervalMinutes']*60) - (end_time - start_time))
            logging.debug('Sleeping for: %ss', round(sleep_time, 2))
            sleep(sleep_time)


class Fallback:
    """ Class for selecting a random fallback endpoint """
    @staticmethod
    def random_server():
        """ Function to select a random TURN server """
        final_server = choice(list(turn['addressMapping'].values()))
        logging.debug('Fallback random_server: %s', final_server)
        global turn_server
        turn_server = final_server

    def random_healthy_server(self):
        """ Function to select a random healthy TURN server """
        healthy_servers = [server for server, data in turn_data.items() if data['healthy'] is True]
        if healthy_servers:
            final_server = choice(healthy_servers)
            logging.debug('Fallback random_healthy_server: %s', final_server)
            global turn_server
            turn_server = final_server
        else:
            self.random_server()


def check_threads():
    """ Function to check if all threads are runnning """
    while True:
        if not all((thread_hc.is_alive(), thread_lb.is_alive(), thread_web.is_alive())):
            logging.critical('Thread is dead, exiting..')
            break
        sleep(2)


if __name__ == "__main__":
    thread_hc = HealthCheck()
    thread_lb = LoadBalancer()
    thread_web = Thread(target=lambda: serve(web_api(),
                                            host=webApi['host'], port=webApi['port'],
                                            threads=webApi['threads']), name='WebApi',
                                            daemon=True)
    thread_hc.start()
    thread_lb.start()
    thread_web.start()
    check_threads()
