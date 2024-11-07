#!/usr/bin/env python3
"""
Module for handling Personal Data
"""
from typing import List
import re
import logging
from os import environ
import mysql.connector

PII_FIELDS = ("name", "email", "phone", "ssn", "password")


class RedactingFormatter(logging.Formatter):
    """
    Custom Formatter to redact PII fields in log messages
    """
    def __init__(self, fields: List[str]):
        self.fields = fields
        super().__init__()

    def format(self, record):
        message = super().format(record)
        return filter_datum(self.fields, "XXX", message, " ")


def filter_datum(fields: List[str], redaction: str,
                 message: str, separator: str) -> str:
    """Returns a log message obfuscated."""
    for f in fields:
        message = re.sub(f'{f}=.*?{separator}',
                         f'{f}={redaction}{separator}', message)
    return message


def get_logger() -> logging.Logger:
    """Returns a Logger Object."""
    logger = logging.getLogger("user_data")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(RedactingFormatter(list(PII_FIELDS)))
    logger.addHandler(stream_handler)

    return logger


def get_db() -> mysql.connector.connection.MySQLConnection:
    """Returns a connector to a MySQL database."""
    # Fetch credentials from environment variables
    username = environ.get("PERSONAL_DATA_DB_USERNAME", "root")
    password = environ.get("PERSONAL_DATA_DB_PASSWORD", "")
    host = environ.get("PERSONAL_DATA_DB_HOST", "localhost")
    db_name = environ.get("PERSONAL_DATA_DB_NAME")

    # Establish and return the database connection
    cnx = mysql.connector.connect(user=username,
                                  password=password,
                                  host=host,
                                  database=db_name)
    return cnx


def main():
    """
    Obtain a database connection using get_db, retrieve all rows
    in the users table, and display each row under a filtered format
    """
    # Get a logger instance
    logger = get_logger()

    # Get a database connection
    db = get_db()
    cursor = db.cursor(dictionary=True)

    # Query the users table
    cursor.execute("SELECT * FROM users")

    # Iterate over the rows in the result
    for row in cursor.fetchall():
        # Create a log message from the row
        log_message = ' '.join([f"{key}={value}" for key, value in row.items()])
        
        # Log the filtered message
        logger.info(log_message)

    # Close the cursor and connection
    cursor.close()
    db.close()


if __name__ == "__main__":
    main()
