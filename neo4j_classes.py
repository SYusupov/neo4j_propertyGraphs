from neo4j import GraphDatabase

class QueryExecutor:
    """executes and outputs the result of a query"""
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def exec_output(self, queries):
        with self.driver.session() as session:
            results = session.execute_read(self.execute_queries, queries)
            for result in results:
                print("The query `{query}` returned {records_count} records in {time} ms.".format(
                    query=result[1].query, records_count=len(result[0]),
                    time=result[1].result_available_after,
                ))
                for record in result[0]:
                    print(record)

    @staticmethod
    def execute_queries(tx, queries):
        results = []
        for query in queries:
            result = tx.run(query)
            records = list(result)
            summary = result.consume()
            results.append((records, summary))
        return results

class Executor:
    """only executes the given query"""
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def exec_output(self, queries):
        with self.driver.session() as session:
            output = session.execute_write(self.load_graph, queries)
            print(output)

    @staticmethod
    def load_graph(tx, queries):
        results = []
        for query in queries:
            results.append(tx.run(query))
            print(results[-1])
        return results