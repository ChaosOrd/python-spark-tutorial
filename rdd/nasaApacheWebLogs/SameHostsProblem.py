from pyspark import SparkContext, SparkConf


def split_by_tab(line: str):
    return line.split('\t')[0]

if __name__ == "__main__":
    '''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
    '''
    conf = SparkConf().setAppName("sameHosts").setMaster("local[1]")
    sc = SparkContext(conf=conf)

    julyFirstLogs = sc.textFile("in/nasa_19950701.tsv").map(split_by_tab)
    augustFirstLogs = sc.textFile("in/nasa_19950801.tsv").map(split_by_tab)

    aggregatedLogLines = julyFirstLogs.intersection(augustFirstLogs)

    cleanLogLines = aggregatedLogLines.filter(lambda host: host != 'host')

    cleanLogLines.saveAsTextFile("out/nasa_logs_same_hosts.csv")
