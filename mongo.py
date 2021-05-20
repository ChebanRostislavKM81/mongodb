from pymongo import MongoClient
from pymongo.collection import Collection
import csv
import logging

log = logging.getLogger(__name__)
logging.basicConfig(filename="logfile.txt",format="%(asctime)s --- %(message)s",level=logging.INFO)
log.info("Start")

def populate(start_table: Collection, lastrow: Collection, csv_filename, year, last_row_num):
    with open(csv_filename, encoding="cp1251") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        lastrow.update_one({}, {"$set": {"year": year}})
        i = 0
        row_list = []
        for row in csv_reader:
            i = i + 1
            if i <= last_row_num:
                continue

            row["Year"] = year
            for j in row:
                try:
                    tmp = float(row[j].replace(",", "."))
                    row[j] = tmp
                except Exception:
                    pass
            row_list.append(row)

            if i % 100 == 0:

                try:
                    if (row_list):
                        start_table.insert_many(row_list)
                        lastrow.update_one({}, {"$set": {"row_number": i}})
                except Exception as exception:
                    raise exception
                
                row_list = []

    if i % 100 != 0:
        try:
            if (row_list):
                start_table.insert_many(row_list)
                lastrow.update_one({}, {"$set": {"row_number": i}})
        except Exception as exception:
            raise exception

def main():
    client = MongoClient(port=27017)
    db = client.database

    collections = db.list_collection_names()
    if "last_row" not in collections:
        lastrow = db["last_row"]
        lastrow.insert_one({"year": 2019,
                                  "row_number": 0})
    start_table, lastrow = db.start_table, db.last_row
    lastrow_data = lastrow.find_one()
    year = lastrow_data["year"]
    row_number =  lastrow_data["row_number"]

    index = pass_list.index(year)
    for year in pass_list[index:]:
        populate(start_table, lastrow, f"Odata{year}File.csv", year, row_number)
        row_number = 0


    query = start_table.aggregate(
        [
            {"$match": {"physTestStatus": "Зараховано"}},
            {"$group": {"_id": {"year": "$Year",
                                "region": "$REGNAME"},
                        "mean": {"$avg": "$physBall100"}}},
        ]
    )
    with open("result.csv", "w", newline='') as csvfile:
        csq_writer = csv.DictWriter(csvfile, fieldnames=["region", "mean", "year"])
        csq_writer.writeheader()
        for row in query:

            row["year"] = row["_id"]["year"]
            row["region"] = row["_id"]["region"]
            del row["_id"]
            csq_writer.writerow(row)
    client.close()
    log.info("Finish")

if __name__ == "__main__":
    pass_list = [2019, 2020]
    main()
