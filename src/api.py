from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
import boto3
import json
import mysql.connector
from pymongo import MongoClient
from datetime import datetime

app = FastAPI(title="Data Lake API")