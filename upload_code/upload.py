from sqlalchemy import create_engine, text

connection_string = "postgresql://project2_db_c203_user:fywne3E3J7jw1LH8XJEhASdE0Ak1mUoW@dpg-d4mi2ehr0fns73a75k80-a.oregon-postgres.render.com/project2_db_c203"
engine = create_engine(connection_string)

with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO project2_table (name, age)
        VALUES ('UploadedUser', 25);
    """))
    conn.commit()

print("Upload complete!")
