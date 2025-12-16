import fitz

def analyze_pdf():
    doc = fitz.open("sample_hcpa.pdf")
    for page in doc:
        text = page.get_text()
        print(f"--- PAGE {page.number + 1} ---")
        print(text[:1000]) # First 1000 chars
        
        if "Sales History" in text:
            print("\n*** FOUND SALES HISTORY ***")
            # Try to print the area around "Sales History"
            idx = text.find("Sales History")
            print(text[idx:idx+500])

if __name__ == "__main__":
    analyze_pdf()

