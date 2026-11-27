from bs4 import BeautifulSoup


def get_rows(file_path):
    with open(file_path, encoding="utf8") as f:
        html = f.read()
    soup = BeautifulSoup(html, "lxml")
    country_table = soup.find("table", {"class": "country-table"})
    return country_table.find_all("tr")


def Q1(file_path):  # DO NOT modify this line
    rows = get_rows(file_path)

    idxs_by_day = {
        "Monday": 0,
        "Tuesday": 1,
        "Wednesday": 2,
        "Thursday": 3,
        "Friday": 4,
        "Saturday": 5,
        "Sunday": 6,
    }

    cnts = [0 for _ in range(7)]

    for row in rows[1:]:
        ls = list(row.children)

        type = ls[7].get_text()
        if type != "National Holiday":
            continue

        day = ls[1].get_text()
        cnts[idxs_by_day[day]] += 1

    return cnts


def Q2(file_path):  # DO NOT modify this line
    rows = get_rows(file_path)

    for row in rows[1:]:
        ls = list(row.children)

        name = ls[5].get_text()

        if "Visakha Bucha" in name:
            return ls[1].get_text()
    else:
        return None


exec(input().strip())  # do not delete this line
