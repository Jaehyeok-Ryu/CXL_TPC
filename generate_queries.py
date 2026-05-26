import subprocess
import os
import re

def generate_queries():
    dbgen_dir = "/home/sawi/cxl_TPC/tpch-dbgen"
    output_dir = "/home/sawi/cxl_TPC/queries"
    os.makedirs(output_dir, exist_ok=True)
    
    for q_id in range(1, 23):
        # Run qgen for the query
        cmd = f"DSS_QUERY=queries ./qgen -d {q_id}"
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=dbgen_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error generating query {q_id}: {result.stderr}")
            continue
            
        sql = result.stdout
        
        # Clean up Oracle database header/comment stuff
        sql = re.sub(r"^-- using default substitutions\s*", "", sql)
        
        # Parse and translate rownum to PostgreSQL LIMIT
        # qgen appends "where rownum <= X;" or similar at the end, sometimes after a semicolon
        rownum_match = re.search(r"where rownum <= (-?\d+);", sql)
        if rownum_match:
            limit_val = int(rownum_match.group(1))
            # Remove the rownum line
            sql = re.sub(r"where rownum <= -?\d+;\s*", "", sql)
            
            # If there is a limit, we should inject it before the final semicolon of the main SELECT statement
            if limit_val > 0:
                # Find the last semicolon and replace it with "LIMIT X;"
                # We search from the end for the last semicolon
                idx = sql.rfind(";")
                if idx != -1:
                    sql = sql[:idx] + f"\nLIMIT {limit_val};" + sql[idx+1:]
        
        # PostgreSQL specific fixes for interval formatting if needed
        # date '1998-12-01' - interval '90' day (3) -> date '1998-12-01' - interval '90' day
        sql = re.sub(r"interval '(\d+)' day \(\d+\)", r"interval '\1' day", sql)
        sql = re.sub(r"interval '(\d+)' month \(\d+\)", r"interval '\1' month", sql)
        sql = re.sub(r"interval '(\d+)' year \(\d+\)", r"interval '\1' year", sql)
        
        # Save query to output_dir
        out_file = os.path.join(output_dir, f"q{q_id}.sql")
        with open(out_file, "w") as f:
            f.write(sql.strip() + "\n")
            
        print(f"Successfully generated and formatted {out_file}")

if __name__ == "__main__":
    generate_queries()
