# Snowflake Cortex Analysis Demo

A complete demonstration of using Snowflake Cortex to analyze customer reviews for a food truck business.  
Includes:  
- Data loading and pivoting with Snowpark
- Prompt engineering and LLM (Cortex) summarization
- Automated theme extraction, email generation, and trend analysis
- Output to DOCX for sharing with stakeholders

## Project Structure

- `src/` — All core Python scripts (Cortex analysis logic and Snowflake utilities)
- `sql/` — Database setup and resource creation SQL scripts
- `config/` — Configuration for secure connections (TOML format)
- `input/` — Input data files (cleaned CSV reviews)
- `output/` — Output documents (Word summaries, analytics)
- `requirements.txt` — Python dependencies

## Quickstart

1. **Configure Snowflake secrets:**  
   - Edit `config/config.toml` and supply your Snowflake and private key details.

2. **Install dependencies:**  
pip install -r requirements.txt

3. **Run the analysis:**  

4. **Review outputs:**  
- Results are in `output/cortex_analysis_output.docx`
- Data pivots and logs will display in your terminal

## Security Notes

- **Never** commit real secrets or keys in config files.
- Use secret managers and `.gitignore` for all private credentials and keys.

## Authors

- *Your Name* (for demo/portfolio purposes)
