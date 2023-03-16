from neo4j_classes import QueryExecutor

if __name__ == "__main__":
    queries = [
        """
        // projection for Page Rank
        CALL gds.graph.project(
        'pageRankGraph',
        'Paper',
        'cites'
        )
        YIELD
        graphName AS graph, nodeProjection, relationshipProjection
        """,

        """
        // Page Rank
        CALL gds.pageRank.stream('pageRankGraph', {
        maxIterations: 50,
        dampingFactor: 0.5
        })
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC, score DESC
        limit 10;
        """,

        """
        // projection for Betweenness Centrality
        CALL gds.graph.project(
        'betweennessGraph',
        'Paper',
        'cites'
        )
        YIELD
        graphName AS graph, nodeProjection, relationshipProjection
        """,
        
        """
        // Betweenness Centrality
        CALL gds.betweenness.stream('betweennessGraph', 
        {samplingSize: 2000, samplingSeed: 891})
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).title AS title, score
        ORDER BY score DESC
        limit 10;
        """,
        """
        // Graph Projection for Node Similarity
        CALL gds.graph.project.cypher(
        'NodeSimilarity',
        'MATCH (n) where n:Author or n:Paper RETURN id(n) AS id',
        'MATCH (n:Author)<-[w:written_by]-(m:Paper) RETURN id(n) AS source, id(m) AS target'
        )
        yield graphName as graph, nodeQuery, nodeCount as nodes, relationshipCount as rels
        """,
        """
        // Node Similarity
        CALL gds.nodeSimilarity.stream('NodeSimilarity', {topK: 5, similarityCutoff: 0.5})
        YIELD node1, node2, similarity
        RETURN gds.util.asNode(node1).name AS Author1, gds.util.asNode(node2).name AS Author2, similarity
        ORDER BY similarity descending, Author1, Author2
        """
    ]
    
    instance = QueryExecutor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output(queries)
    instance.close()