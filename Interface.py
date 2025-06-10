#!/usr/bin/python2.7
#
# Interface for the assignement
#

import mysql.connector

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='root', password='123456', dbname='mysql'):
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='123456',
        database=dbname
    )


def loadratings(ratingstablename, ratingsfilepath, openconnection): 

    con = openconnection
    cur = con.cursor()
    
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE " + ratingstablename + 
               " (userid INTEGER, movieid INTEGER, rating FLOAT)")
    
    with open(ratingsfilepath, 'r') as f:
        for line in f:
            userid, _, movieid, _, rating, _, _ = line.strip().split(':')
            cur.execute("INSERT INTO " + ratingstablename + 
                       " (userid, movieid, rating) VALUES (%s, %s, %s)",
                       (int(userid), int(movieid), float(rating)))
    
    cur.close()
    con.commit()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Drop existing partitions if any
    for i in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS " + RANGE_TABLE_PREFIX + str(i))
    
    # Create and fill partitions
    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        
        # Create partition table
        cur.execute("CREATE TABLE " + table_name + 
                   " (userid INTEGER, movieid INTEGER, rating FLOAT)")
        
        # Insert data
        if i == 0:
            cur.execute("INSERT INTO " + table_name + 
                       " SELECT userid, movieid, rating FROM " + ratingstablename + 
                       " WHERE rating >= %s AND rating <= %s", (minRange, maxRange))
        else:
            cur.execute("INSERT INTO " + table_name + 
                       " SELECT userid, movieid, rating FROM " + ratingstablename + 
                       " WHERE rating > %s AND rating <= %s", (minRange, maxRange))
    
    cur.close()
    con.commit()


# def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

#     con = openconnection
#     cur = con.cursor()
#     RROBIN_TABLE_PREFIX = 'rrobin_part'
    
#     for i in range(numberofpartitions):
#         cur.execute("DROP TABLE IF EXISTS " + RROBIN_TABLE_PREFIX + str(i))
    
#     for i in range(numberofpartitions):
#         table_name = RROBIN_TABLE_PREFIX + str(i)
#         cur.execute("CREATE TABLE " + table_name + 
#                    " (userid INTEGER, movieid INTEGER, rating FLOAT)")

#     cur.execute("SELECT userid, movieid, rating FROM " + ratingstablename)
#     rows = cur.fetchall()

#     for idx, row in enumerate(rows):
#         table_name = RROBIN_TABLE_PREFIX + str(idx % numberofpartitions)
#         cur.execute("INSERT INTO " + table_name + 
#                    " (userid, movieid, rating) VALUES (%s, %s, %s)", row)
    
#     con.commit()
#     cur.close()



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS " + table_name)
        cur.execute("CREATE TABLE " + table_name + " (userid INTEGER, movieid INTEGER, rating FLOAT)")
        sql_insert = (
            "INSERT INTO `" + table_name + "` (userid, movieid, rating) "
            "SELECT userid, movieid, rating "
            "FROM ("
            "   SELECT userid, movieid, rating, "
            "   ROW_NUMBER() OVER (ORDER BY userid ASC) AS rnum "
            "   FROM `" + ratingstablename + "`"
            ") AS numbered "
            "WHERE MOD(numbered.rnum - 1, " + str(numberofpartitions) + ") = " + str(i) + ";"
        )
        cur.execute(sql_insert)

    con.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    # Insert into main table
    cur.execute("INSERT INTO " + ratingstablename + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    # Get total number of rows and partitions
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename)
    total_rows = cur.fetchone()[0]
    
    # Count partitions
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s",
                (RROBIN_TABLE_PREFIX + '%',))
    numberofpartitions = cur.fetchone()[0]
    
    # Calculate target partition
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    
    # Insert into partition
    cur.execute("INSERT INTO " + table_name + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    cur.close()
    con.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Count partitions
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s",
                (RANGE_TABLE_PREFIX + '%',))
    numberofpartitions = cur.fetchone()[0]
    
    # Calculate partition
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    
    # Insert into main table and partition
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("INSERT INTO " + ratingstablename + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    cur.execute("INSERT INTO " + table_name + 
               " (userid, movieid, rating) VALUES (%s, %s, %s)",
               (userid, itemid, rating))
    
    cur.close()
    con.commit()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s",
                (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count
