from neo4j import GraphDatabase

class Executor:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def exec_output(self):
        with self.driver.session() as session:
            results = session.execute_read(self.load_graph)
            for result in results:
                print("The query `{query}` returned {records_count} records in {time} ms.".format(
                    query=result[1].query, records_count=len(result[0]),
                    time=result[1].result_available_after,
                ))
                for record in result[0]:
                    print(record)

    @staticmethod
    def load_graph(tx):
        queries = [ 
                    """
                    // B1
                    match (c:Conference)--()--(p2:Paper)<--(p:Paper)
                    with c, p2, count(p) as numCitations
                    order by numCitations desc
                    with c.name as class, collect({paper:p2.title, score:numCitations}) as citations
                    return class,citations[0..3] as top3
                    order by class
                    """,
                    """
                    // B2
                    match (c:Conference)--(e:Edition)--(p:Paper)-[:written_by]->(a:Author)
                    with a, c, count(distinct e) as numEditions
                    where numEditions >=4
                    return a.name, c.name, numEditions
                    order by numEditions desc
                    """,
                    """
                    // B3
                    Match (j:Journal)--(v:Volume)--(p:Paper)<-[c:cites]-(p2:Paper)
                    WITH j, p, toInteger(p2.year) as citationYear, toInteger(p.year) as publishingYear, count(c) as citations
                    where publishingYear < citationYear AND citationYear<=publishingYear+2
                    with j, citationYear, collect(citations) as citationList, count(distinct p) as nPublished
                    with j, citationYear, nPublished, reduce(totalCitations = 0, item in citationList | totalCitations + item) AS totalCitations
                    return j.name, citationYear, totalCitations/nPublished as ImpactFactor
                    order by j.name, citationYear
                    """,
                    """
                    // B4
                    match (a:Author)<-[:written_by]-(p2:Paper)<--(p:Paper)
                    with a, p2, count(p) as paperCitations order by paperCitations desc
                    with a, count(p2) as papersPerAuthor, collect(paperCitations) as p2Citations
                    with a, papersPerAuthor, [temp in range(0,papersPerAuthor-1) where p2Citations[temp] >= papersPerAuthor ] as h
                    return a.name, papersPerAuthor, last(h)+1 as indexH order by papersPerAuthor desc
                    """
        ]
        results = []
        for query in queries:
            result = tx.run(query)
            records = list(result)
            summary = result.consume()
            results.append((records, summary))
        return results


if __name__ == "__main__":
    instance = Executor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output()
    instance.close()