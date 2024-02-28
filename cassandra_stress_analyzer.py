import subprocess
import argparse
from threading import Thread
from statistics import mean, stdev


class CassandraStressThread(Thread):

    def __init__(self, container_name: str, duration: int, node_ip: str) -> None:
        super().__init__()
        self.container_name = container_name
        self.duration = duration
        self.node_ip = node_ip
        self.status = None

    def run(self) -> None:
        print(f'Starting CassandraStressThread with duration: {self.duration}', flush=True)
        self.status = subprocess.run(['docker', 'exec', self.container_name, 'cassandra-stress', 'write',
                                      f'duration={self.duration}s', '-rate threads=10', '-node', self.node_ip],
                                     capture_output=True, text=True)
        if self.status.returncode == 0:
            print(f'CassandraStressThread with duration: {self.duration} finished successfully.', flush=True)
        else:
            print(f'CassandraStressThread with duration: {self.duration} finished with error code: '
                  f'{self.status.returncode}. \nstderr:\n {self.status.stderr}', flush=True)


class CassandraStressAnalyzer:

    def __init__(self, container_name: str, durations: list, node_ip: str) -> None:
        self.container_name = container_name
        self.durations = durations
        self.node_ip = node_ip
        self.threads = []
        self.results = []

    @staticmethod
    def get_results(log: str) -> dict:
        results = log.split('Results:')[1].strip('\nEND\n')
        results_dict = {}
        for line in results.splitlines():
            name, value = line.split(':', 1)
            if name.strip() == 'Total operation time':
                results_dict[name.strip()] = value.strip()
            else:
                results_dict[name.strip()] = float(value.strip().split(' ')[0].replace(',', ''))
        return results_dict

    def run(self) -> None:
        for duration in self.durations:
            thread = CassandraStressThread(self.container_name, duration, self.node_ip)
            self.threads.append(thread)
            thread.start()
        for thread in self.threads:
            thread.join()
        self.analyze_and_print_results()

    def analyze_and_print_results(self) -> None:
        for thread in self.threads:
            if thread.status.returncode == 0:
                self.results.append(self.get_results(thread.status.stdout))
        print('\nTest summary:\n')
        print('Number of stress processes that ran:', len(self.threads))
        print('Number of stress processes that finished successfully:', len(self.results))
        if len(self.results) > 0:
            print('Calculated aggregation of "Op rate" (sum) [op/s]:', int(sum([d['Op rate'] for d in self.results])))
            print('Calculated average of "Latency mean" (average) [ms]:',
                  mean([d['Latency mean'] for d in self.results]))
            print('Calculated average of "Latency 99th percentile" (average) [ms]:',
                  mean([d['Latency 99th percentile'] for d in self.results]))
            if len(self.results) >= 2:
                print('Standard deviation calculation of all "Latency max" results [ms]:',
                      stdev([d['Latency max'] for d in self.results]))
            else:
                print('Standard deviation calculation of "Latency max" not available for only 1 measurement.')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("container_name", help='Name of container with ScyllaDB cluster')
    parser.add_argument("node_ip", help='IP address of ScyllaDB node in cluster')
    parser.add_argument("duration", type=int, nargs='+', help='Duration of single stress test thread. '
                                                              'Can be used multiple times for multiple threads.')
    args = parser.parse_args()

    cassandra_stress_analyzer = CassandraStressAnalyzer(args.container_name, args.duration, args.node_ip)
    cassandra_stress_analyzer.run()
