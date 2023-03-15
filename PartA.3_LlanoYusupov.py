from neo4j import GraphDatabase

class Executor:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def exec_output(self, path):
        with self.driver.session() as session:
            output = session.execute_write(self.load_graph, path)
            print(output)

    @staticmethod
    def load_graph(tx, path):
        queries = [ 
                    """
                    // D1: Adding affiliations for each author (affiliations.csv)
                    LOAD CSV WITH HEADERS FROM 'file:///%s/affiliations.csv' AS line
                    merge (af:Affiliation {name: line.AffiliationName, type: line.Type})
                    merge (a:Author {name: line.Author})
                    with af, a, line
                    merge (af)<-[:belongs_to]-(a);
                    """%(path),
                    """
                    // D2: Adding approval to Review
                    MATCH (p:Paper)-[r:REVIEWS]-(a:Author)
                    SET r.approved=true
                    RETURN r
                    """,
                    """
                    // D3: Adding approval to Paper-Edition
                    match (p:Paper) <-[old:contains] - (e)
                    create (p)-[new:applies_to]->(e)
                    delete old
                    set new.approved=true
                    set new.num_reviewers=3;
                    """
        ]
        results = []
        for query in queries:
            results.append(tx.run(query))
            print(results[-1])
        return results


if __name__ == "__main__":
    instance = Executor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output("home/sayyor/Desktop/neo4j-community-4.2.19/lab-property-graphs")
    instance.close()