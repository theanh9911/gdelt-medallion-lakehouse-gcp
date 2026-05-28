import os
import requests
import zipfile
from io import BytesIO

def download_gdelt_sample():
    # 1. Duong dan thu muc
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    print("[GDELT] Fetching GDELT master file list...")
    master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    
    try:
        response = requests.get(master_url, timeout=10)
        response.raise_for_status()
        
        # 2. Tim file GKG v2 gan nhat
        lines = response.text.splitlines()
        gkg_url = None
        
        # Doc tu duoi len de lay file moi nhat
        for line in reversed(lines):
            parts = line.split()
            if len(parts) == 3:
                url = parts[2]
                if ".gkg.csv.zip" in url:
                    gkg_url = url
                    break
        
        if not gkg_url:
            print("[GDELT] Khong tim thay file GKG v2 nao.")
            return
        
        filename = os.path.basename(gkg_url)
        csv_filename = filename.replace(".zip", "")
        dest_csv_path = os.path.join(data_dir, csv_filename)
        
        print(f"[GDELT] Da tim thay file GKG moi nhat: {gkg_url}")
        
        # Kiem tra neu file da ton tai thi khong tai lai
        if os.path.exists(dest_csv_path):
            print(f"[GDELT] File GKG da ton tai tai: {dest_csv_path}")
            print(f"[GDELT] Size: {os.path.getsize(dest_csv_path) / (1024*1024):.2f} MB")
            return
            
        print(f"[GDELT] Dang tai {filename}...")
        file_response = requests.get(gkg_url, stream=True)
        file_response.raise_for_status()
        
        # 3. Giai nen truc tiep tu stream
        print("[GDELT] Dang giai nen truc tiep vao thu muc data/...")
        zip_file = zipfile.ZipFile(BytesIO(file_response.content))
        zip_file.extractall(data_dir)
        
        print("[GDELT] Tai xuong va giai nen thanh cong!")
        print(f"[GDELT] File: {dest_csv_path}")
        print(f"[GDELT] Size: {os.path.getsize(dest_csv_path) / (1024*1024):.2f} MB")
        
    except Exception as e:
        print(f"[GDELT] Da xay ra loi: {e}")

if __name__ == "__main__":
    download_gdelt_sample()
