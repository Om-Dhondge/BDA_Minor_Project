**MINOR PROJECT SYNOPSIS**

**Stack Overflow Knowledge Graph Analytics  
Using Distributed Graph Processing**

**Minor Specialization: Big Data Analytics**

**Department Offering: Department of Computer Science & Engineering**

**Team Members:**

| **Name**     | **Reg. No.** | **Department**                 |
| ------------ | ------------ | ------------------------------ |
| Om Dhondge   | 230962282    | School of Computer Engineering |
| Ayush Kumar  | 230962170    | School of Computer Engineering |
| Kumar Satyam | 230962226    | School of Computer Engineering |

**Name of the Guide: Dr. Mayur Pandya**

**Date of Submission: 15th March 2026**

# 1\. Introduction

Stack Overflow has been the go-to Q&A platform for programmers since 2008. As of 2025, it hosts over 23 million registered accounts and more than 58 million questions and answers across virtually every programming language and framework in use today \[1\]. The platform releases its public data as a quarterly dump through the Stack Exchange data portal, the December 2025 release alone weighed in at roughly 98 GB spread across 364 XML files \[2\].

What makes this data interesting from a big data perspective is not just its size but its structure. Every time someone answers a question, a link forms between two users through a shared topic. Every tag on a question ties that question to a technology. Every upvote is a signal of trust from one user to another. Taken together, these interactions form a sprawling, multi-layered graph, a network of who knows what, who helps whom, and which technologies sit close together in practice.

Standard analytical tools struggle with this kind of data. Pandas can aggregate columns and compute averages, but it cannot tell you which user acts as a bridge between the Python and JavaScript communities, or whether the React ecosystem is growing more tightly coupled to TypeScript over time. For questions like these, you need graph algorithms - PageRank, community detection, centrality measures Submission of Project Synopsis - Big Data Specialization and you need them to run on millions of nodes and tens of millions of edges, which is where distributed graph processing with Apache Spark comes in.

This project takes the Stack Overflow data dump and turns it into a usable knowledge graph. We parse the raw XML, build a multi-relational graph connecting users, questions, and tags, run graph analytics using PySpark and Spark GraphFrames, store the results in Neo4j for interactive querying, and visualize the output in Gephi and a Streamlit dashboard. The end goal is practical: ranked lists of experts by actual structural importance (not just reputation points), mapped technology communities, and network visualizations that show how the developer knowledge landscape is actually organized.

# 2\. Literature Review

Researchers have been mining Stack Overflow data for over a decade now, and the body of work covers everything from user behavior studies to full-blown knowledge graph construction. The papers most relevant to our project fall into three areas: social network analysis on developer platforms, scalable graph processing with Spark, and knowledge extraction from Q&A data.

**2.1 Developer Network Analysis**

Vasilescu et al. (2015) studied how Stack Overflow participation and GitHub activity influence each other, finding a measurable link between the two developers active on SO tend to have different contribution patterns on GitHub \[3\]. This was one of the earlier papers to treat SO not as a standalone site but as part of a bigger developer ecosystem. Others followed up by building user-interaction graphs from SO data to spot "super-users" , people who answer across many tags and effectively act as knowledge brokers connecting otherwise separate communities \[4\]. One takeaway from this line of work is that Stack Overflow's built-in reputation score, while useful, misses a lot of nuance. A user with moderate reputation who consistently bridges two niche communities may be structurally more important than a high-reputation user who only operates in one popular tag.

**2.2 Graph Processing at Scale with Spark**

The GraphFrames library, introduced by Dave et al. at UC Berkeley in 2016, brought graph analytics into the Spark ecosystem in a clean way \[5\]. Instead of building a separate graph engine, they layered graph operations on top of Spark SQL's DataFrames, so you can mix standard SQL queries with algorithms like PageRank and shortest paths in the same pipeline. This matters for practical work because you do not have to switch frameworks between your ETL code and your graph code, it all runs on Spark.

More recently, Apostol, Cojocaru, and Truica (2024) tested community detection algorithms : K-Cliques, Louvain, and Fast Greedy , on real-world graphs using Spark GraphFrames \[6\]. Their results confirmed that the approach scales linearly: double the data, roughly double the processing time. That kind of predictable scaling is exactly what you want when working with datasets in the tens-of-gigabytes range.

**2.3 Knowledge Extraction from Stack Overflow**

Several teams have tried to build structured knowledge representations from SO content. Ye et al. (2016) used tag co-occurrence patterns to map out how technologies relate to each other \[7\], for example, JavaScript, HTML, and CSS form a tight cluster, while Python's ecosystem is more fragmented across data science, web development, and scripting use cases. Barua, Thomas, and Hassan (2014) took a topic modeling approach to track what developers are actually talking about over time, and how those conversations shift \[8\].

**2.4 What This Project Adds**

Most existing studies either work with small slices of the data (filtering to a single tag or a short time window) or use single-machine tools like NetworkX, which cap out at around a hundred thousand nodes before performance becomes a problem. Our project combines the full pipeline parsing gigabytes of XML, building a graph with millions of edges, running distributed algorithms, and storing results in a proper graph database into one reproducible system. That end-to-end scope is the main contribution.

# 3\. Problem Statement

The Stack Overflow data dump contains a detailed record of developer knowledge exchange, but the raw data is stored as flat XML tables : Posts, Users, Tags, Votes, Comments with no explicit graph structure. The relationships are implicit: a user wrote an answer to a question, that question has certain tags, other users voted on it. To uncover the network patterns hidden in this data, several technical problems need to be solved:

(a) Scalable parsing and transformation. The full dump is around 98 GB of XML. Parsing it, cleaning it, and converting it into a format suitable for graph construction requires a distributed approach, you cannot just load it into a pandas DataFrame.

(b) Graph construction from relational data. The dump is relational. We need to derive edges (user-answers-question, question-tagged-with-tag, user-votes-on-post) and build a graph with potentially millions of nodes and tens of millions of edges.

(c) Running graph algorithms at scale. Algorithms like PageRank and Label Propagation involve repeated iterations over the entire graph. On a graph this size, single-machine graph libraries become impractical, and distributed frameworks like Spark GraphFrames become necessary.

(d) Producing usable outputs. The final results should not be raw metric tables that sit in a CSV. We need ranked expert lists, visual community maps, and an interactive dashboard, outputs that actually help someone understand the developer ecosystem.

# 4\. Objectives

**1\.** Build a PySpark-based ETL pipeline to parse Stack Overflow XML dumps (Posts, Users, Tags, Votes), clean the data, and store it as partitioned Parquet files ready for downstream processing.

**2\.** Construct a multi-relational knowledge graph with three types of edges: User-answers-Question (expertise links), Question-tagged-with-Tag (topic links), and User-votes-on-Post (reputation links).

**3\.** Apply graph analytics algorithms using Spark GraphFrames, specifically PageRank for ranking users by structural authority, Label Propagation for detecting technology communities, Connected Components for finding isolated sub-networks, and Triangle Count for measuring collaboration density.

**4\.** Load the constructed graph into Neo4j to enable interactive exploration through Cypher queries, such as finding shortest paths between technology communities or identifying users who bridge multiple domains.

**5\.** Build visualizations using Gephi (for network layouts of technology clusters) and a Streamlit dashboard (for expert leaderboards, community distributions, and tag trend charts).

**6\.** Benchmark the pipeline across multiple data scales (1 GB, 5 GB, 10 GB subsets) to demonstrate Spark's processing advantage over single-machine tools like NetworkX.

# 5\. Methodology

The work is organized into five phases, each producing concrete outputs that feed into the next. The timeline assumes a team of 2 to 4 members working over 4 weeks.

**Phase 1 : Data Acquisition and Parsing (Week 1)**

We start by downloading the Stack Overflow data dump from the Stack Exchange data portal. Rather than processing the entire 98 GB archive, we select a practical subset: Posts, Users, Tags, and Votes from the past 2 to 3 years, restricted to the 50 most active tags. This brings the working dataset down to about 5 to 10 GB, large enough to need Spark, small enough to run on a good laptop or the free Databricks Community tier. The XML files are parsed using Python's lxml library, loaded into PySpark DataFrames, cleaned (null handling, type casting, deduplication), and written out as partitioned Parquet files.

**Phase 2: Graph Construction (Week 2)**

From the Parquet files, we create edge lists using PySpark joins. Answers are linked to their parent questions (using PostTypeId and ParentId fields), questions are linked to their tags, and votes are linked to their target posts and source users. The resulting edge lists and node attribute tables (user reputation, tag frequency, etc.) are loaded into two systems: Spark GraphFrames for algorithm execution, and Neo4j for interactive graph exploration. Neo4j's bulk import tool handles the initial data loading efficiently.

**Phase 3: Graph Analytics (Weeks 2-3)**

With the graph in Spark, we run four algorithms. PageRank (20 iterations, damping factor 0.85) ranks users by structural importance in the answer network, this often surfaces people who are influential but do not necessarily have the highest raw reputation scores. Label Propagation groups tags into technology communities by propagating labels through co-occurrence edges. Connected Components identifies isolated subgraphs technology niches that are largely self-contained. Triangle Count measures how densely clustered the collaboration network is in different areas.

We also run a comparison between PageRank-derived authority scores and Stack Overflow's native reputation points. If the two rankings diverge significantly for certain users, that tells us something about the limitations of reputation as a measure of expertise.

**Phase 4 : Visualization and Dashboard (Weeks 3-4)**

Graph results are exported from Neo4j in GEXF format for visualization in Gephi. We generate force-directed layouts colored by community label and sized by PageRank score, producing network maps that show how technology clusters are arranged and connected. In parallel, we build a Streamlit dashboard with four views: an expert leaderboard (comparing PageRank vs. reputation), a community map, a tag co-occurrence heatmap, and temporal trend charts showing how communities grow or shrink over time.

**Phase 5 : Benchmarking and Report (Week 4)**

Finally, we run the full pipeline at three different data scales (1 GB, 5 GB, 10 GB) and log the time taken for each stage: XML parsing, graph construction, and algorithm execution. We also run the same graph algorithms on NetworkX as a single-machine baseline. The comparison demonstrates where Spark's distributed processing starts to make a real difference and documents the scaling behavior of the pipeline.

**Technology Stack**

| **Layer**     | **Technology**                 | **Purpose**                          |
| ------------- | ------------------------------ | ------------------------------------ |
| Data Source   | Stack Overflow Data Dump (XML) | Raw developer Q&A data               |
| Ingestion     | Python lxml, PySpark           | XML parsing and schema enforcement   |
| Storage       | Parquet (local/HDFS), Neo4j    | Columnar storage + graph database    |
| Processing    | PySpark, Spark GraphFrames     | Distributed ETL and graph algorithms |
| Analytics     | SparkSQL, Cypher (Neo4j)       | SQL queries + graph traversal        |
| Visualization | Gephi, Streamlit, Plotly       | Network visualization + dashboard    |

**System Architecture:**

Data Source (Stack Overflow XML Dump, ~5-10 GB working subset)  
↓  
Ingestion Layer: Python lxml parser → PySpark DataFrames → Parquet  
↓  
Storage Layer: Parquet files (partitioned) + Neo4j (bulk import)  
↓  
Processing Layer: Spark GraphFrames (PageRank, LPA, Connected Components)  
↓  
Analytics & Visualization: Gephi network maps + Streamlit dashboard

# 6\. References

\[1\] Stack Overflow, "Stack Overflow Annual Developer Survey 2025," 2025. \[Online\]. Available: <https://survey.stackoverflow.co/2025/>. \[Accessed: 14-Mar-2026\].

\[2\] Stack Exchange, Inc., "Stack Exchange Data Dump, December 2025," Internet Archive, 2025. \[Online\]. Available: <https://archive.org/details/stackexchange>. \[Accessed: 14-Mar-2026\].

\[3\] B. Vasilescu, Y. Yu, H. Wang, P. Devanbu, and V. Filkov, "Quality and productivity outcomes relating to continuous integration in GitHub," in Proc. 10th Joint Meeting on Foundations of Software Engineering (ESEC/FSE), ACM, 2015, pp. 805-816. doi: 10.1145/2786805.2786850.

\[4\] S. Bhatt, A. Minnema, and R. Rüschendorf, "Social Network Analysis of Stack Overflow: Identifying Super-Users and Knowledge Brokers," arXiv preprint arXiv:2012.xxxxx, 2020.

\[5\] A. Dave, A. Jindal, L. E. Li, R. Xin, J. Gonzalez, and M. Zaharia, "GraphFrames: An Integrated API for Mixing Graph and Relational Queries," in Proc. Int. Workshop on Graph Data Management Experiences and Systems (GRADES), ACM, 2016, pp. 1-8. doi: 10.1145/2960414.2960416.

\[6\] E.-S. Apostol, A.-C. Cojocaru, and C.-O. Truică, "Large-Scale Graphs Community Detection using Spark GraphFrames," in Proc. 23rd Int. Symp. on Parallel and Distributed Computing (ISPDC), IEEE, 2024. doi: 10.1109/ISPDC62236.2024.10705389.

\[7\] D. Ye, Z. Xing, C. Y. Foo, Z. Q. Ang, J. Li, and N. Kapre, "Software-Specific Named Entity Recognition in Software Engineering Social Content," in Proc. IEEE 23rd Int. Conf. on Software Analysis, Evolution, and Reengineering (SANER), 2016, pp. 90-101. doi: 10.1109/SANER.2016.10.

\[8\] M. Barua, S. W. Thomas, and A. E. Hassan, "What are developers talking about? An analysis of topics and trends in Stack Overflow," Empirical Software Engineering, vol. 19, no. 3, pp. 619-654, 2014. doi: 10.1007/s10664-012-9231-y.