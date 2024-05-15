import re
import fitz  # PyMuPDF
import pandas as pd


def extract_words_from_pdf(file_path, output_csv_path):
    # Strict pattern to match words, part of speech, and CEFR levels
    pattern = r'\b([a-zA-Z]+(?:, [a-zA-Z]+)?)\s+([a-zA-Z.]+)\s+(A[12]|B[12]|C[12])\b'

    # Function to extract words using PyMuPDF
    def extract_words_pymupdf(file_path, pattern):
        doc = fitz.open(file_path)
        extracted_data = []

        for page in doc:
            text = page.get_text("text")
            if text:
                matches = re.findall(pattern, text)
                extracted_data.extend(matches)

        doc.close()
        return extracted_data

    # Extract words
    extracted_words = extract_words_pymupdf(file_path, pattern)

    # Remove duplicates and sort
    unique_extracted_words = sorted(set(extracted_words), key=lambda x: x[0].lower())

    # Create a DataFrame
    df = pd.DataFrame(unique_extracted_words, columns=["Word", "Part of Speech", "CEFR Level"])

    # Save to a CSV file
    df.to_csv(output_csv_path, index=False)
    return output_csv_path


# Example usage
if __name__ == "__main__":
    input_pdf_path = 'path_to_your_pdf_file.pdf'  # Replace with your PDF file path
    output_csv_path = 'extracted_words.csv'  # Replace with your desired output CSV file path
    result = extract_words_from_pdf(input_pdf_path, output_csv_path)
    print(f"Extracted data saved to: {result}")
