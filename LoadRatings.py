# LoadRatings.py

import mysql.connector
import os

# Thông tin kết nối MySQL (chỉnh lại user/password nếu khác)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'movielens_db',
    'raise_on_warnings': True
}

# Mở kết nối và cursor toàn cục
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

def LoadRatings(file_path):
    """
    Đọc file ratings.dat và chèn vào bảng Ratings (MySQL).
    file_path: đường dẫn tuyệt đối đến ratings.dat (ví dụ "C:\\Users\\YourName\\Downloads\\ratings.dat").
    """

    # 1. Kiểm tra file tồn tại
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    # 2. Xóa và tạo lại bảng Ratings (nếu cần)
    cursor.execute("DROP TABLE IF EXISTS Ratings;")
    create_table = """
    CREATE TABLE Ratings (
        UserID  INT   NOT NULL,
        MovieID INT   NOT NULL,
        Rating  FLOAT NOT NULL,
        PRIMARY KEY (UserID, MovieID)
    );
    """
    cursor.execute(create_table)
    conn.commit()

    # 3. Chuẩn bị batch insert
    insert_query = "INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    batch_size = 10000
    batch = []

    # 4. Mở file và đọc lần lượt
    #    KHÔNG gọi LOAD DATA INFILE vì delimiter là "::", MySQL không hỗ trợ trực tiếp.
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split("::")
            if len(parts) < 3:
                continue
            user_id = int(parts[0])
            movie_id = int(parts[1])
            rating = float(parts[2])
            batch.append((user_id, movie_id, rating))

            # Khi đủ batch_size, chèn 1 lần
            if len(batch) >= batch_size:
                cursor.executemany(insert_query, batch)
                conn.commit()
                batch = []

        # Chèn dòng còn lại
        if batch:
            cursor.executemany(insert_query, batch)
            conn.commit()

    print("Đã load xong dữ liệu vào bảng Ratings.")


def Range_Partition(ratings_table, N):
    """
    Tạo N partition (range) dựa trên cột Rating.
    Tên partition: range_part0, range_part1, ..., range_part{N-1}.
    """

    # 1. Xóa các bảng range_part nếu tồn tại
    for i in range(N):
        cursor.execute(f"DROP TABLE IF EXISTS range_part{i};")
    conn.commit()

    # 2. Tạo N bảng mới
    for i in range(N):
        create_query = f"""
        CREATE TABLE range_part{i} (
            UserID  INT   NOT NULL,
            MovieID INT   NOT NULL,
            Rating  FLOAT NOT NULL
        );
        """
        cursor.execute(create_query)
    conn.commit()

    # 3. Tính interval
    interval = 5.0 / N

    # 4. Chèn dữ liệu cho từng partition
    for i in range(N):
        lower = i * interval
        upper = (i + 1) * interval
        if i == 0:
            # Partition 0: Rating >= lower AND Rating <= upper
            insert_query = f"""
            INSERT INTO range_part{i} (UserID, MovieID, Rating)
            SELECT UserID, MovieID, Rating
            FROM {ratings_table}
            WHERE Rating >= {lower} AND Rating <= {upper};
            """
        else:
            # Partition i>0: Rating > lower AND Rating <= upper
            insert_query = f"""
            INSERT INTO range_part{i} (UserID, MovieID, Rating)
            SELECT UserID, MovieID, Rating
            FROM {ratings_table}
            WHERE Rating > {lower} AND Rating <= {upper};
            """
        cursor.execute(insert_query)
        conn.commit()

    print(f"Đã tạo và chèn dữ liệu vào {N} bảng range_part (0 đến {N-1}).")


def RoundRobin_Partition(ratings_table, N):
    """
    Tạo N partition (round robin) dựa trên thứ tự đọc.
    Tên partition: rrobin_part0, rrobin_part1, ..., rrobin_part{N-1}.
    Tạo thêm bảng RR_Metadata (NextPartition).
    """

    # 1. Xóa bảng metadata và các rrobin_part cũ nếu có
    cursor.execute("DROP TABLE IF EXISTS RR_Metadata;")
    for i in range(N):
        cursor.execute(f"DROP TABLE IF EXISTS rrobin_part{i};")
    conn.commit()

    # 2. Tạo bảng RR_Metadata (chứa NextPartition)
    cursor.execute("""
    CREATE TABLE RR_Metadata (
        NextPartition INT NOT NULL
    );
    """)
    cursor.execute("INSERT INTO RR_Metadata (NextPartition) VALUES (0);")
    conn.commit()

    # 3. Tạo N bảng rrobin_part{i}
    for i in range(N):
        create_query = f"""
        CREATE TABLE rrobin_part{i} (
            UserID  INT   NOT NULL,
            MovieID INT   NOT NULL,
            Rating  FLOAT NOT NULL
        );
        """
        cursor.execute(create_query)
    conn.commit()

    # 4. Duyệt và chèn round robin
    select_query = f"SELECT UserID, MovieID, Rating FROM {ratings_table};"
    cursor.execute(select_query)

    batch_size = 10000
    count = 0
    insert_statements = [
        f"INSERT INTO rrobin_part{i} (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
        for i in range(N)
    ]
    buffer = []

    # Dùng fetchmany để tránh load toàn bộ vào RAM
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for (u, m, r) in rows:
            target = count % N
            buffer.append((u, m, r, target))
            count += 1

            if len(buffer) >= batch_size:
                # Nhóm buffer theo partition
                partition_bins = {i: [] for i in range(N)}
                for (u2, m2, r2, t2) in buffer:
                    partition_bins[t2].append((u2, m2, r2))
                # Chèn mỗi partition
                for i in range(N):
                    if partition_bins[i]:
                        cursor.executemany(insert_statements[i], partition_bins[i])
                conn.commit()
                buffer = []

    # Chèn phần dư
    if buffer:
        partition_bins = {i: [] for i in range(N)}
        for (u2, m2, r2, t2) in buffer:
            partition_bins[t2].append((u2, m2, r2))
        for i in range(N):
            if partition_bins[i]:
                cursor.executemany(insert_statements[i], partition_bins[i])
        conn.commit()

    print(f"Đã tạo và chèn dữ liệu vào {N} bảng rrobin_part (0 đến {N-1}).")


def Range_Insert(ratings_table, UserID, MovieID, Rating):
    """
    Chèn một dòng mới vào bảng ratings_table và partition range tương ứng.
    """

    # 1. Chèn vào bảng gốc
    insert_main = f"INSERT INTO {ratings_table} (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(insert_main, (UserID, MovieID, Rating))
    conn.commit()

    # 2. Xác định N: số bảng range_part%
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name LIKE 'range_part%%';
    """, (DB_CONFIG['database'],))
    N = cursor.fetchone()[0]
    if N == 0:
        raise Exception("Chưa có partition range. Vui lòng gọi Range_Partition trước.")

    # 3. Tính interval
    interval = 5.0 / N

    # 4. Tìm chỉ số partition
    idx = None
    for i in range(N):
        low = i * interval
        high = (i + 1) * interval
        if i == 0:
            if Rating >= low and Rating <= high:
                idx = 0
                break
        else:
            if Rating > low and Rating <= high:
                idx = i
                break
    if idx is None:
        idx = N - 1

    # 5. Chèn vào partition tương ứng
    insert_range = f"INSERT INTO range_part{idx} (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(insert_range, (UserID, MovieID, Rating))
    conn.commit()

    print(f"Đã chèn vào range_part{idx}.")


def RoundRobin_Insert(ratings_table, UserID, MovieID, Rating):
    """
    Chèn một dòng mới vào bảng ratings_table và partition rrobin tương ứng.
    """

    # 1. Chèn vào bảng gốc
    insert_main = f"INSERT INTO {ratings_table} (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(insert_main, (UserID, MovieID, Rating))
    conn.commit()

    # 2. Lấy NextPartition từ RR_Metadata
    cursor.execute("SELECT NextPartition FROM RR_Metadata;")
    result = cursor.fetchone()
    if result is None:
        raise Exception("Bảng RR_Metadata không tồn tại. Vui lòng gọi RoundRobin_Partition trước.")
    next_part = result[0]

    # 3. Xác định N: số bảng rrobin_part%
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name LIKE 'rrobin_part%%';
    """, (DB_CONFIG['database'],))
    N = cursor.fetchone()[0]
    if N == 0:
        raise Exception("Chưa có partition rrobin. Vui lòng gọi RoundRobin_Partition trước.")

    # 4. Chèn vào partition tương ứng
    insert_rr = f"INSERT INTO rrobin_part{next_part} (UserID, MovieID, Rating) VALUES (%s, %s, %s)"
    cursor.execute(insert_rr, (UserID, MovieID, Rating))
    conn.commit()

    # 5. Cập nhật NextPartition
    new_next = (next_part + 1) % N
    cursor.execute("UPDATE RR_Metadata SET NextPartition = %s", (new_next,))
    conn.commit()

    print(f"Đã chèn vào rrobin_part{next_part}, NextPartition được cập nhật là {new_next}.")


# Nếu chạy trực tiếp file này, ví dụ:
if __name__ == "__main__":
    import sys

    # Ví dụ chạy: python LoadRatings.py "C:\\Users\\YourName\\Downloads\\ratings.dat"
    if len(sys.argv) < 2:
        print("Usage: python LoadRatings.py <absolute_path_to_ratings.dat>")
        sys.exit(1)

    path = sys.argv[1]
    print("Bắt đầu LoadRatings...")
    LoadRatings(path)

    # Ví dụ test nhanh partition
    N = 3
    print(f"Chạy Range_Partition với N={N}...")
    Range_Partition("Ratings", N)

    M = 4
    print(f"Chạy RoundRobin_Partition với N={M}...")
    RoundRobin_Partition("Ratings", M)
