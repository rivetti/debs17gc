# debs17gc

IE&M - Technion solution to the DEBS 2017 Grand Challenge 


This repository contains the implementation of the IE&M - Technion solution to the DEBS 2017 Grand Challenge (https://doi.org/10.1145/3093742.3096342)

We present the details of the solution in the paper Nicolo Rivetti, Yann Busnel, and Avigdor Gal. 2017. FlinkMan: Anomaly Detection in Manufacturing Equipment with Apache Flink: Grand Challenge. In Proceedings of the 11th ACM International Conference on Distributed and Event-based Systems (DEBS '17). 

Abstract:
We present a (soft) real-time event-based anomaly detection application for manufacturing equipment, built on top of the general purpose stream processing framework Apache Flink. The anomaly detection involves multiple CPUs and/or memory intensive tasks, such as clustering on large time-based window and parsing input data in RDF-format. The main goal is to reduce end-to-end latencies, while handling high input throughput and still provide exact results. Given a truly distributed setting, this challenge also entails careful task and/or data parallelization and balancing. We propose FlinkMan, a system that offers a generic and efficient solution, which maximizes the usage of available cores and balances the load among them. We illustrates the accuracy and efficiency of FlinkMan, over a 3-step pipelined data stream analysis, that includes clustering, modeling and querying.
