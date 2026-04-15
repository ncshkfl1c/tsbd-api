from flask import Flask, request, jsonify
import pandas as pd
import base64
import io
from datetime import datetime
import os

app = Flask(__name__)

# ===== HOME (tránh lỗi 404)
@app.route('/')
def home():
    return "TSBD API is running"

# ===== FIX SERIAL DATE (giống VBA)
def FixSerialDate(strInput):
    if pd.isna(strInput) or strInput == "":
        return ""

    arrParts = str(strInput).split(";")
    result = []

    for tempValue in arrParts:
        tempValue = tempValue.strip()

        if tempValue.isnumeric():
            try:
                dt = pd.to_datetime(float(tempValue), unit='D', origin='1899-12-30')
                tempValue = dt.strftime("%d/%m/%Y")
            except:
                pass

        result.append(tempValue)

    return "; ".join(result)


def process_tsbd(file_old, file_new, file_map=None):
    df_old = pd.read_excel(file_old).fillna("")
    df_new = pd.read_excel(file_new).fillna("")

    dictAll = {}
    dictNewOnly = {}

    # ===== LOAD FILE NEW
    for i in range(len(df_new)):
        colID = str(df_new.at[i, 'L']).strip()
        if colID != "":
            dictAll[colID] = i
            dictNewOnly[colID] = i

    # ===== XỬ LÝ OLD
    rows_keep = []

    for i in range(len(df_old)-1, -1, -1):
        colID = str(df_old.at[i, 'J']).strip()

        if colID in dictAll:
            rNew = dictAll[colID]

            try:
                ngayGiaHanNew = pd.to_datetime(df_new.at[rNew, 'U']).strftime("%d/%m/%Y")
            except:
                ngayGiaHanNew = str(df_new.at[rNew, 'U']).strip()

            try:
                ngayMuon = pd.to_datetime(df_new.at[rNew, 'O']).strftime("%d/%m/%Y")
            except:
                ngayMuon = str(df_new.at[rNew, 'O']).strip()

            lichSuCu = str(df_old.at[i, 'U'])

            if lichSuCu.startswith("'"):
                lichSuCu = lichSuCu[1:]

            lichSuCu = FixSerialDate(lichSuCu)

            if ngayGiaHanNew and ngayGiaHanNew != ngayMuon:
                if ngayGiaHanNew not in lichSuCu:
                    lichSuCu = f"{lichSuCu}; {ngayGiaHanNew}" if lichSuCu else ngayGiaHanNew

            if lichSuCu:
                count = len(lichSuCu.replace(" ", "").split(";"))
            else:
                count = ""

            df_old.at[i, 'U'] = lichSuCu
            df_old.at[i, 'T'] = count

            rows_keep.append(df_old.loc[i])
            dictNewOnly.pop(colID, None)

    df_old = pd.DataFrame(rows_keep[::-1])

    # ===== THÊM MỚI
    for colID, rNew in dictNewOnly.items():
        new_row = {}

        new_row['B'] = df_new.at[rNew, 'D']
        new_row['C'] = df_new.at[rNew, 'E']
        new_row['D'] = df_new.at[rNew, 'F']
        new_row['E'] = df_new.at[rNew, 'G']
        new_row['J'] = df_new.at[rNew, 'L']

        try:
            new_row['M'] = pd.to_datetime(df_new.at[rNew, 'O']).strftime("%d/%m/%Y")
        except:
            new_row['M'] = df_new.at[rNew, 'O']

        new_row['O'] = df_new.at[rNew, 'P']

        try:
            new_row['S'] = pd.to_datetime(df_new.at[rNew, 'T']).strftime("%d/%m/%Y")
        except:
            new_row['S'] = df_new.at[rNew, 'T']

        try:
            u = pd.to_datetime(df_new.at[rNew, 'U'])
            o = pd.to_datetime(df_new.at[rNew, 'O'])
            if u != o:
                new_row['U'] = u.strftime("%d/%m/%Y")
        except:
            pass

        valX = df_new.at[rNew, 'X']
        new_row['Y'] = str(valX).zfill(10) if str(valX).isdigit() else valX

        lichSuCu = FixSerialDate(new_row.get('U', ""))
        new_row['T'] = len(lichSuCu.replace(" ", "").split(";")) if lichSuCu else ""

        df_old = pd.concat([df_old, pd.DataFrame([new_row])], ignore_index=True)

    # ===== MAP FILE
    if file_map:
        df_map = pd.read_excel(file_map).fillna("")
        dictMap = {str(df_map.at[i, 'C']).strip(): i for i in range(len(df_map))}

        for i in range(len(df_old)):
            key = str(df_old.at[i, 'B']).strip()
            if key in dictMap:
                rMap = dictMap[key]
                df_old.at[i, 'AA'] = df_map.at[rMap, 'D']
                df_old.at[i, 'Z'] = df_map.at[rMap, 'E']

    # ===== QUÁ HẠN
    today = datetime.today()
    df_old['AB'] = ""

    for i in range(len(df_old)):
        try:
            ngayTra = pd.to_datetime(df_old.at[i, 'S'])
            df_old.at[i, 'AB'] = "Qua han" if ngayTra < today else "Chua qua han"
        except:
            df_old.at[i, 'AB'] = "Chua qua han"

    return df_old


# ===== API
@app.route('/process-tsbd', methods=['POST'])
def api():
    try:
        data = request.json

        if not data:
            return jsonify({"error": "No JSON received"}), 400

        file_old = base64.b64decode(data['old'])
        file_new = base64.b64decode(data['new'])
        file_map = base64.b64decode(data['map']) if data.get('map') else None

        df = process_tsbd(
            io.BytesIO(file_old),
            io.BytesIO(file_new),
            io.BytesIO(file_map) if file_map else None
        )

        output = io.BytesIO()
        df.to_excel(output, index=False)

        return jsonify({
            "file": base64.b64encode(output.getvalue()).decode()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===== RUN
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
