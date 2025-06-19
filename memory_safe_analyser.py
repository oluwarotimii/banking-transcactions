import streamlit as st
import os
import tempfile
import gc
from datetime import datetime, timedelta
from collections import defaultdict
import io
import re
import csv
import pdfplumber
import time

# Configure page
st.set_page_config(
    page_title="Memory-Safe Bank Transaction Analyzer",
    page_icon="🏦",
    layout="wide"
)

class MemorySafeBankAnalyzer:
    """
    Memory-safe bank transaction analyzer with stateless processing
    Prevents memory leaks through aggressive cleanup and stateless operations
    """
    
    def __init__(self, duplicate_days=3, amount_threshold=1000, similarity_threshold=80):
        self.duplicate_days = duplicate_days
        self.amount_threshold = amount_threshold
        self.similarity_threshold = similarity_threshold
        self.processed_files = {}
    
    def extract_account_id_from_filename(self, filename):
        """Extract Account ID from PDF filename"""
        try:
            return os.path.splitext(filename)[0]
        except:
            return filename
    
    def process_single_page_stateless(self, pdf_path, page_num, account_id, source_file):
        """
        Process a single PDF page with complete memory cleanup
        Returns list of transactions from this page only
        """
        page_transactions = []
        page = None
        table = None
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_num < len(pdf.pages):
                    page = pdf.pages[page_num]
                    table = page.extract_table()
                    
                    if table and len(table) > 1:
                        for row_idx, row in enumerate(table[1:]):
                            if row and len(row) >= 6:
                                transaction = self._parse_table_row_safe(row, account_id, source_file, page_num, row_idx)
                                if transaction:
                                    page_transactions.append(transaction)
                    
                    # Immediately destroy page objects
                    del table
                    del page
                    
        except Exception as e:
            st.warning(f"Error processing page {page_num + 1}: {str(e)}")
        
        finally:
            # Force cleanup of any remaining objects
            gc.collect()
        
        return page_transactions
    
    def _parse_table_row_safe(self, row, account_id, source_file, page_num, row_idx):
        """Parse table row with error handling"""
        try:
            date_time = str(row[0] or "").strip()
            narration = str(row[1] or "").strip()
            reference = str(row[2] or "").strip()
            debit = self._clean_amount(str(row[3] or "0"))
            credit = self._clean_amount(str(row[4] or "0"))
            balance = self._clean_amount(str(row[5] or "0"))
            
            if not date_time or not narration:
                return None
            
            try:
                dt = self._parse_datetime(date_time)
            except:
                return None
            
            beneficiary = self._extract_beneficiary(narration)
            
            return {
                'transaction_id': f"{account_id}_{dt.strftime('%Y%m%d_%H%M%S')}_{page_num}_{row_idx}",
                'account_id': account_id,
                'source_file': source_file,
                'datetime': dt.isoformat(),
                'date': dt.strftime('%d/%m/%Y'),
                'time': dt.strftime('%H:%M:%S'),
                'narration': narration,
                'beneficiary': beneficiary,
                'reference': reference,
                'debit_amount': debit,
                'credit_amount': credit,
                'balance': balance,
                'transaction_type': 'debit' if debit > 0 else 'credit'
            }
        except:
            return None
    
    def extract_pdf_stateless(self, uploaded_file, output_csv_path, pages_per_batch=100, pause_seconds=7.0):
        """
        Stateless PDF extraction - processes in tiny batches with complete cleanup
        """
        account_id = self.extract_account_id_from_filename(uploaded_file.name)
        
        # Create temporary PDF file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_pdf:
            uploaded_file.seek(0)
            temp_pdf.write(uploaded_file.read())
            temp_pdf_path = temp_pdf.name
        
        total_transactions = 0
        total_pages = 0
        
        # Progress containers
        progress_container = st.empty()
        status_container = st.empty()
        
        # CSV headers
        csv_headers = ['transaction_id', 'account_id', 'source_file', 'datetime', 'date', 'time', 
                      'narration', 'beneficiary', 'reference', 'debit_amount', 'credit_amount', 
                      'balance', 'transaction_type']
        
        try:
            # Get total pages count first
            with pdfplumber.open(temp_pdf_path) as pdf:
                total_pages = len(pdf.pages)
            
            # Initialize CSV file
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                writer.writeheader()
            
            # Process in ultra-small batches
            for batch_start in range(0, total_pages, pages_per_batch):
                batch_end = min(batch_start + pages_per_batch, total_pages)
                batch_transactions = []
                
                batch_num = (batch_start // pages_per_batch) + 1
                total_batches = ((total_pages - 1) // pages_per_batch) + 1
                
                status_container.info(f"Processing batch {batch_num}/{total_batches} (Pages {batch_start+1}-{batch_end})")
                
                # Process each page individually with complete cleanup
                for page_num in range(batch_start, batch_end):
                    page_transactions = self.process_single_page_stateless(
                        temp_pdf_path, page_num, account_id, uploaded_file.name
                    )
                    
                    batch_transactions.extend(page_transactions)
                    
                    # Update progress
                    progress = (page_num + 1) / total_pages
                    progress_container.progress(progress, 
                                              text=f"Page {page_num + 1}/{total_pages} - {len(batch_transactions)} transactions in batch")
                    
                    # Cleanup page transactions immediately
                    del page_transactions
                    gc.collect()
                
                # Write batch to CSV and immediately cleanup
                if batch_transactions:
                    with open(output_csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
                        for txn in batch_transactions:
                            writer.writerow(txn)
                    
                    total_transactions += len(batch_transactions)
                    status_container.success(f"Batch {batch_num} completed: {len(batch_transactions)} transactions")
                
                # Complete batch cleanup
                del batch_transactions
                gc.collect()
                
                # Memory recovery pause - longer pause for better cleanup
                if batch_end < total_pages:
                    time.sleep(pause_seconds)
                    status_container.info(f"Deep memory cleanup pause: {pause_seconds}s - Processing {pages_per_batch} pages per batch")
            
            progress_container.success(f"Extraction complete: {total_transactions} transactions from {total_pages} pages")
            
        except Exception as e:
            st.error(f"Error during stateless extraction: {str(e)}")
            total_transactions = 0
            
        finally:
            # Cleanup temporary PDF
            try:
                os.unlink(temp_pdf_path)
            except:
                pass
        
        # Store file info
        self.processed_files[uploaded_file.name] = {
            'account_id': account_id,
            'transactions_count': total_transactions,
            'pages_processed': total_pages
        }
        
        return total_transactions
    
    def load_csv_in_chunks(self, csv_path, chunk_size=2000):
        """Load CSV in small chunks to prevent memory buildup"""
        all_transactions = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                chunk = []
                
                for row in reader:
                    # Convert numeric fields
                    row['debit_amount'] = float(row['debit_amount']) if row['debit_amount'] else 0.0
                    row['credit_amount'] = float(row['credit_amount']) if row['credit_amount'] else 0.0
                    row['balance'] = float(row['balance']) if row['balance'] else 0.0
                    
                    chunk.append(row)
                    
                    if len(chunk) >= chunk_size:
                        all_transactions.extend(chunk)
                        del chunk
                        chunk = []
                        gc.collect()
                
                # Add remaining
                if chunk:
                    all_transactions.extend(chunk)
                    del chunk
                    gc.collect()
                    
        except Exception as e:
            st.error(f"Error loading CSV: {str(e)}")
            return []
        
        return all_transactions
    
    def _clean_amount(self, amount_str):
        """Clean and convert amount string to float"""
        try:
            cleaned = re.sub(r'[^\d.-]', '', str(amount_str))
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0
    
    def _parse_datetime(self, date_time_str):
        """Parse datetime with multiple format support"""
        formats = [
            '%d/%m/%Y, %H:%M:%S', '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y, %H:%M:%S', '%d-%m-%Y %H:%M:%S',
            '%d/%m/%Y', '%d-%m-%Y',
            '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_time_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse datetime: {date_time_str}")
    
    def _extract_beneficiary(self, narration):
        """Extract beneficiary name from narration"""
        if not narration:
            return "Unknown"
        
        # Simple extraction logic
        parts = narration.split()
        if len(parts) > 2:
            return ' '.join(parts[1:3])
        return parts[0] if parts else "Unknown"
    
    def _calculate_similarity(self, str1, str2):
        """Calculate string similarity percentage"""
        try:
            from fuzzywuzzy import fuzz
            return fuzz.ratio(str1 or "", str2 or "")
        except:
            # Fallback simple similarity
            str1, str2 = (str1 or "").lower(), (str2 or "").lower()
            if str1 == str2:
                return 100
            if str1 in str2 or str2 in str1:
                return 80
            return 0

def main():
    """Main Streamlit application"""
    st.title("🏦 Memory-Safe Bank Transaction Analyzer")
    st.markdown("**Ultra-efficient processing for large bank statement PDFs**")
    
    # Sidebar settings
    st.sidebar.header("⚙️ Memory Management Settings")
    
    pages_per_batch = st.sidebar.selectbox(
        "Pages per Batch",
        options=[50, 100, 150, 200],
        index=1,
        help="Larger batches for faster processing with memory-safe design"
    )
    
    pause_duration = st.sidebar.slider(
        "Cleanup Pause (seconds)",
        min_value=1.0, max_value=10.0, value=7.0, step=1.0,
        help="Time between batches for memory cleanup"
    )
    
    chunk_size = st.sidebar.selectbox(
        "CSV Loading Chunk Size",
        options=[1000, 2000, 3000, 5000],
        index=1,
        help="Size of chunks when loading CSV back into memory"
    )
    
    # Initialize analyzer
    analyzer = MemorySafeBankAnalyzer(
        duplicate_days=3,
        amount_threshold=1000,
        similarity_threshold=80
    )
    
    # File upload
    st.header("📁 Upload Bank Statement PDFs")
    st.info("Upload PDF bank statements for memory-safe processing")
    
    uploaded_files = st.file_uploader(
        "Choose PDF files",
        type=['pdf'],
        accept_multiple_files=True,
        help="Select bank statement PDF files"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files uploaded")
        
        # Show file info
        for file in uploaded_files:
            account_id = analyzer.extract_account_id_from_filename(file.name)
            st.write(f"📁 **{file.name}** → Account: **{account_id}** → Size: **{round(file.size / 1024, 2)} KB**")
        
        if st.button("🚀 Start Memory-Safe Processing", type="primary"):
            st.header("📄 Processing Phase")
            
            all_csv_files = []
            total_extracted = 0
            
            # Process each file individually with complete cleanup
            for file_idx, uploaded_file in enumerate(uploaded_files):
                st.subheader(f"Processing {uploaded_file.name} ({file_idx + 1}/{len(uploaded_files)})")
                
                # Create unique CSV for this file
                csv_filename = f"extracted_{analyzer.extract_account_id_from_filename(uploaded_file.name)}_{int(time.time())}.csv"
                
                with st.spinner(f"Extracting from {uploaded_file.name}..."):
                    extracted_count = analyzer.extract_pdf_stateless(
                        uploaded_file,
                        csv_filename,
                        pages_per_batch=pages_per_batch,
                        pause_seconds=pause_duration
                    )
                
                if extracted_count > 0:
                    all_csv_files.append(csv_filename)
                    total_extracted += extracted_count
                    st.success(f"✅ {extracted_count} transactions extracted from {uploaded_file.name}")
                else:
                    st.warning(f"⚠️ No transactions found in {uploaded_file.name}")
                
                # Force cleanup between files
                del uploaded_file
                gc.collect()
            
            if all_csv_files:
                st.header("📊 Analysis Phase")
                st.info(f"Loading {total_extracted} transactions for analysis...")
                
                # Combine all CSV files efficiently
                all_transactions = []
                for csv_file in all_csv_files:
                    transactions = analyzer.load_csv_in_chunks(csv_file, chunk_size)
                    all_transactions.extend(transactions)
                    
                    # Cleanup CSV file
                    os.remove(csv_file)
                    del transactions
                    gc.collect()
                
                st.success(f"✅ Loaded {len(all_transactions)} transactions for analysis")
                
                # Display basic statistics
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    debits = [t for t in all_transactions if t['transaction_type'] == 'debit']
                    st.metric("Total Debits", len(debits))
                
                with col2:
                    credits = [t for t in all_transactions if t['transaction_type'] == 'credit']
                    st.metric("Total Credits", len(credits))
                
                with col3:
                    total_amount = sum(t['debit_amount'] + t['credit_amount'] for t in all_transactions)
                    st.metric("Total Amount", f"₦{total_amount:,.2f}")
                
                # Show sample transactions
                st.subheader("Sample Transactions")
                for i, txn in enumerate(all_transactions[:10]):
                    with st.expander(f"Transaction {i+1} - {txn['transaction_type'].upper()}"):
                        st.write(f"**Account:** {txn['account_id']}")
                        st.write(f"**Date:** {txn['date']} {txn['time']}")
                        st.write(f"**Amount:** ₦{txn['debit_amount'] + txn['credit_amount']:,.2f}")
                        st.write(f"**Beneficiary:** {txn['beneficiary']}")
                        st.write(f"**Narration:** {txn['narration']}")
                
                st.success("✅ Memory-safe processing completed successfully!")
            else:
                st.error("❌ No transactions could be extracted from any files")

if __name__ == "__main__":
    main()