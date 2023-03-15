from neo4j_classes import Executor, QueryExecutor

if __name__ == "__main__":
    queries1 = [ 
        """
        // Step 1, removing initial topics of target keywords
        match path=(k:Keyword)-[e:part_of]-(t:Topic)
        where k.word in [
        'Optical data processing','data processing','data modeling','data storage and data querying ',
        'big data','data management','data storage and data querying','indexing']
        delete e
        """,

        """
        // Step 1, adding the target keywords to one topic
        match (k:Keyword)
        where k.word in [
        'Optical data processing','data processing','data modeling','data storage and data querying ',
        'big data','data management','data storage and data querying','indexing']
        merge (t:Topic {name: "Databases"})
        with k, t
        merge (k)-[:part_of]->(t);
        """,

        """
        // Step 2, adding more papers of 1 journal to DBs topic
        match (j:Journal {name:"IEEE Robotics and Automation Letters"})--()--(p:Paper)
        match (k:Keyword {word:"big data"})
        merge (p)-[:relates_to]->(k)
        """,
        
        """
        // Step 2, adding more papers of 1 conference to DBs topic
        match (c:Conference {name:"ASEE Annual Conference and Exposition, Conference Proceedings"})--()--(p:Paper)
        match (k:Keyword {word:"indexing"})
        merge (p)-[:relates_to]->(k)
        """
    ]
    queries2 = [
        """
        // Step 2, confirming that 1 journal has more than 90% of DBs papers
        match path=(j:Journal)--()--(p:Paper)
        with j, count(distinct p) as nAllPapers
        match path=(j:Journal)--()--(p1:Paper)--()--(t:Topic {name:'Databases'})
        with j, nAllPapers, count(distinct p1) as nDBPapers
        with j, nDBPapers/nAllPapers as percDB
        where percDB >=0.9
        return j.name, percDB
        """,
        """
        // Step 2, confirming that 1 conference has more than 90% of DBs papers
        match path=(c:Conference)--()--(p:Paper)
        with c, count(distinct p) as nAllPapers
        match path=(c:Conference)--()--(p1:Paper)--()--(t:Topic {name:'Databases'})
        with c, nAllPapers, count(distinct p1) as nDBPapers
        with c, nDBPapers/nAllPapers as percDB
        where percDB >=0.9
        return c.name, percDB
        """,
        """
        // Step 3, finding top-3 papers of conferences of the community by # of citations from the same community
        with ["IEEE Robotics and Automation Letters"] as DBjs, ["ASEE Annual Conference and Exposition, Conference Proceedings"] as DBcs
        match (c1:Conference)--()--(p:Paper)<-[:cites]-(p1:Paper)--()--(c2:Conference)
        match (p:Paper)<-[:cites]-(p2:Paper)--()--(j:Journal)
        where j.name in DBjs and c1.name in DBcs and c2.name in DBcs
        with p,c1, count(distinct p2) + count(distinct p1) as countDBCites
        order by countDBCites descending
        with c1,collect({paper:p.title, NDBcites: countDBCites}) as cites
        return c1.name as conference, cites[0..3] as top3
        """,
        """
        // Step 3, same for journals
        with ["IEEE Robotics and Automation Letters"] as DBjs, ["ASEE Annual Conference and Exposition, Conference Proceedings"] as DBcs
        match (j1:Journal)--()--(p:Paper)<-[:cites]-(p1:Paper)--()--(j2:Journal)
        match (p:Paper)<-[:cites]-(p2:Paper)--()--(c:Conference)
        where j1.name in DBjs and j2.name in DBjs and c.name in DBcs
        with p,j1, count(distinct p2)+count(distinct p1) as countDBCites
        order by countDBCites descending
        with j1,collect({paper:p.title, NDBcites: countDBCites}) as cites
        return j1.name as journal, cites[0..3] as top3
        """,
        """
        // Step 4, adding more papers to authors
        match (p:Paper)
        match (a:Author)
        where a.name in ["Bowman K.E.", "Jensen C.G."] and
        p.title = "Optimization and implementation of synthetic basis feature descriptor on FPGA"
        merge (p)-[:written_by {corresponding_author:True}]->(a);
        """,
        """
        // Step 4, adding more papers to authors
        match (p:Paper)
        match (a:Author)
        where a.name in [" Crane N.", "Weidman J.E.", " Farnsworth C.B."] and
        p.title = "Cross platform usability: Evaluating computing tasks performed on multiple platforms"
        merge (p)-[:written_by {corresponding_author:True}]->(a);
        """,
        """
        // Step 4, getting Gurus of the journals of DB community
        with ["IEEE Robotics and Automation Letters"] as DBjs, ["ASEE Annual Conference and Exposition, Conference Proceedings"] as DBcs
        match (j1:Journal)--()--(p:Paper)<-[:cites]-(p1:Paper)--()--(j2:Journal)
        match (p:Paper)<-[:cites]-(p2:Paper)--()--(c:Conference)
        where j1.name in DBjs and j2.name in DBjs and c.name in DBcs
        with p,j1, count(distinct p2)+count(distinct p1) as countDBCites
        order by countDBCites descending
        with j1,collect(p.title) as cites
        with cites[0..3] as top3
        match (p:Paper)--(a:Author)
        where p.title in top3
        with a, count(p) as nP
        order by nP descending
        where nP>=2
        return a.name, nP
        """,
        """
        // Step 4, getting Gurus of the conferences of DB community
        with ["IEEE Robotics and Automation Letters"] as DBjs, ["ASEE Annual Conference and Exposition, Conference Proceedings"] as DBcs
        match (c1:Conference)--()--(p:Paper)<-[:cites]-(p1:Paper)--()--(c2:Conference)
        match (p:Paper)<-[:cites]-(p2:Paper)--()--(j:Journal)
        where j.name in DBjs and c1.name in DBcs and c2.name in DBcs
        with p,count(distinct p2)+count(distinct p1) as countDBCites
        order by countDBCites descending
        with collect(p.title) as cites
        with cites[0..3] as top3
        match (p:Paper)--(a:Author)
        where p.title in top3
        with a, count(p) as nP
        order by nP descending
        return a.name, nP
        """
    ]
    
    instance = Executor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output(queries1)
    instance.close()

    queryInst = QueryExecutor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output(queries2)
    instance.close()