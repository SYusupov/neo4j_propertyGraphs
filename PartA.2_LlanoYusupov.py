from neo4j_classes import Executor

if __name__ == "__main__":
    path = "home/sayyor/Desktop/neo4j-community-4.2.19/lab-property-graphs"
    queries = [ 
        """
        // Create Paper - Author Relationship (Original Dataset)
        LOAD CSV WITH HEADERS FROM 'file:///%s/main_data.csv' AS line
        merge (p:Paper {title:line.Title, year:line.Year, abstract:line.Abstract})
        with p, line
        unwind split(line.Authors, ",") As author
        merge (a:Author {name:author})
        merge (p)-[:written_by{corresponding_author:FALSE}]->(a);
        """%(path),

        """// Add Keywords to Papers (Original dataset)
        LOAD CSV WITH HEADERS FROM 'file:///%s/main_data.csv' AS line
        merge (p:Paper {title:line.Title, year:line.Year, abstract:line.Abstract, pages:line.Pages})
        with p, line
        unwind split(line.IndexKeywords, ";") As keys
        merge (k:Keyword {word:keys})
        merge (p)-[:relates_to]->(k);"""%(path),

        """// Set Corresponding Author
        MATCH (p:Paper)-[r:written_by]-(a:Author)
        WITH p, min(id(a)) as minID
        MATCH (p:Paper)-[r:written_by]-(a:Author)
        WITH p, id(a) as id, r
        where id = minID
        set r.corresponding_author=TRUE
        return r;""",

        """// Creating Journals, Volumes and their relations
        LOAD CSV WITH HEADERS FROM 'file:///%s/journals.csv' AS line
        merge (j:Journal {name: line.Name})
        merge (v:Volume {name: line.Version})
        merge (p:Paper {title:line.Title})
        with p, j, v, line
        merge (p)<-[:contains]-(v)
        merge (j)-[:releases]->(v);"""%(path),

        """// Creating Conferences, Editions and their relations
        LOAD CSV WITH HEADERS FROM 'file:///%s/conferences.csv' AS line
        merge (c:Conference {name: line.Name})
        merge (e:Edition {name: line.Version})
        merge (p:Paper {title:line.Title})
        with p, c, e, line
        merge (p)<-[:contains]-(e)
        merge (c)-[:releases]->(e);"""%(path),

        """// Adding reviewers
        LOAD CSV WITH HEADERS FROM 'file:///%s/main_data.csv' AS line
        unwind split(line.Reviewers, ",") As reviewer
        match (p:Paper {title:line.Title, year:line.Year, abstract:line.Abstract, pages:line.Pages})
        match (a:Author {name:reviewer})
        merge (p)<-[r:REVIEWS]-(a);"""%(path),

        """// Add citations (each paper cites from 1 to 20 citations)
        // range of how many citations a paper has
        WITH range(1,20) as citedRange
        // for each paper p1, get all papers p2 that are not itself
        MATCH (p1:Paper)
        MATCH (p2:Paper)
        where p2.title <> p1.title
        // group all p2 to variable papers
        WITH collect(p2) as papers, citedRange, p1
        // randomly pick number of citations in the range, use that to get a number of random hobbies
        WITH p1, apoc.coll.randomItems(papers, apoc.coll.randomItem(citedRange)) as papers
        // create relationships
        FOREACH (paper in papers | CREATE (p1)<-[:cites]-(paper));""",

        """// removing cycled citations
        match path=(p:Paper)<-[r:cites]-(p1:Paper)<-[:cites]-(p:Paper)
        delete r;""",

        """// Adding topics for each keyword (topics.csv)
        LOAD CSV WITH HEADERS FROM 'file:///%s/topics.csv' AS line
        merge (t:Topic {name: line.Topic})
        merge (k:Keyword {word: line.Keyword})
        with t, k, line
        merge (k)-[:part_of]->(t);"""%(path)
    ]
    instance = Executor("bolt://localhost:7687", "neo4j", "neo4j")
    instance.exec_output(queries)
    instance.close()