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
        """
    ]
    
    instance = QueryExecutor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output(queries)
    instance.close()