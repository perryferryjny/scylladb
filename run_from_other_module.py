from cassandra_stress_analyzer import CassandraStressAnalyzer

container_name = 'some-scylla'
durations = [*range(1, 11)]
node_ip = '172.17.0.2'

cassandra_stress_analyzer = CassandraStressAnalyzer(container_name, durations, node_ip)
cassandra_stress_analyzer.run()
