import streamlit as st
import pandas as pd
import pdfplumber
import os
import tempfile
import gc
from datetime import datetime
from collections import defaultdict
import xlsxwriter
import io

# Configure page
st.set_page_config(
    page_title="Bank Transaction Analyzer - Table Based",
    page_icon="🏦",
    layout="wide"
)

class TableBasedAnalyzer:
    """Fast table-based PDF analyzer with memory optimization"""
    
    def __init__(self, duplicate_days=3, amount_threshold=1000, similarity_threshold=80):
        self.duplicate_days = duplicate_days
        self.amount_threshold = amount_threshold
        self.similarity_threshold = similarity_threshold
        self.temp_files = []
    
    def extract_transactions_from_pdf(self, uploaded_file, start_page=0, batch_size=50):
        """Extract transactions from table-structured PDF pages"""
        transactions = []
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            uploaded_file.seek(0)
            temp_file.write(uploaded_file.read())
            temp_file_path = temp_file.name
            self.temp_files.append(temp_file_path)
        
        try:
            with pdfplumber.open(temp_file_path) as pdf:
                total_pages = len(pdf.pages)
                end_page = min(start_page + batch_size, total_pages) if batch_size else total_pages
                
                progress_container = st.empty()
                
                for page_num in range(start_page, end_page):
                    try:
                        page = pdf.pages[page_num]
                        table = page.extract_table()
                        
                        if table and len(table) > 1:  # Has header and data
                            # Process table rows (skip header)
                            for row in table[1:]:
                                if row and len(row) >= 6:  # Ensure minimum columns
                                    transaction = self._parse_table_row(row)
                                    if transaction:
                                        transactions.append(transaction)
                        
                        # Update progress
                        progress = (page_num - start_page + 1) / (end_page - start_page)
                        progress_container.progress(progress, 
                                                  text=f"Processing page {page_num + 1} of {total_pages}")
                        
                        # Memory cleanup every 10 pages
                        if page_num % 10 == 0:
                            gc.collect()
                            
                    except Exception as e:
                        st.warning(f"Could not process page {page_num + 1}: {str(e)}")
                        continue
                
                progress_container.empty()
                
        except Exception as e:
            st.error(f"Error processing PDF: {str(e)}")
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file_path)
                if temp_file_path in self.temp_files:
                    self.temp_files.remove(temp_file_path)
            except:
                pass
        
        return transactions
    
    def _parse_table_row(self, row):
        """Parse a single table row into transaction dictionary"""
        try:
            # Expected columns: Date & Time, Narration, Reference, Debit, Credit, Balance
            date_time = str(row[0] or "").strip()
            narration = str(row[1] or "").strip()
            reference = str(row[2] or "").strip()
            debit = self._clean_amount(str(row[3] or "0"))
            credit = self._clean_amount(str(row[4] or "0"))
            balance = self._clean_amount(str(row[5] or "0"))
            
            if not date_time or not narration:
                return None
            
            # Parse datetime
            try:
                dt = self._parse_datetime(date_time)
            except:
                return None
            
            # Extract beneficiary from narration
            beneficiary = self._extract_beneficiary(narration)
            
            return {
                'datetime': dt,
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
            
        except Exception as e:
            return None
    
    def _clean_amount(self, amount_str):
        """Clean and convert amount string to float"""
        try:
            clean_amount = str(amount_str).replace(',', '').replace('₦', '').replace('NGN', '').strip()
            return float(clean_amount) if clean_amount else 0.0
        except:
            return 0.0
    
    def _parse_datetime(self, date_time_str):
        """Parse various datetime formats"""
        formats = [
            '%d/%m/%Y, %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y, %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%d/%m/%Y',
            '%d-%m-%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_time_str, fmt)
            except:
                continue
        
        # If all else fails, try basic parsing
        raise ValueError(f"Cannot parse datetime: {date_time_str}")
    
    def _extract_beneficiary(self, narration):
        """Extract beneficiary name from narration"""
        # Clean common prefixes
        cleaners = [
            'TRANSFER TO:',
            'PAYMENT TO:',
            'TRF TO:',
            'PAY:',
            'TRANSFER',
            'PAYMENT'
        ]
        
        beneficiary = narration.upper()
        for cleaner in cleaners:
            beneficiary = beneficiary.replace(cleaner, '').strip()
        
        # Take first part before reference/slash
        beneficiary = beneficiary.split('/')[0].split('|')[0].split('REF:')[0].strip()
        
        return beneficiary[:50]  # Limit length
    
    def analyze_transactions(self, all_transactions_df):
        """Analyze transactions for refunds and duplicates"""
        results = {
            'refunds': [],
            'duplicates': [],
            'unmatched_debits': [],
            'summary': {}
        }
        
        # Convert to DataFrame if not already
        if not isinstance(all_transactions_df, pd.DataFrame):
            df = pd.DataFrame(all_transactions_df)
        else:
            df = all_transactions_df
        
        if df.empty:
            return results
        
        # Analyze refunds
        st.info("Analyzing refunds...")
        results['refunds'] = self._detect_refunds(df)
        
        # Analyze duplicates
        st.info("Detecting duplicates...")
        results['duplicates'] = self._detect_duplicates(df)
        
        # Get unmatched debits
        results['unmatched_debits'] = self._get_unmatched_debits(df, results['refunds'])
        
        # Generate summary
        results['summary'] = self._generate_summary(df, results)
        
        return results
    
    def _detect_refunds(self, df):
        """Detect refunded transactions"""
        refunds = []
        debits = df[df['transaction_type'] == 'debit'].copy()
        credits = df[df['transaction_type'] == 'credit'].copy()
        
        progress_container = st.empty()
        
        for idx, debit in debits.iterrows():
            # Look for matching credits after the debit
            matching_credits = credits[
                (credits['datetime'] > debit['datetime']) &
                (credits['credit_amount'] == debit['debit_amount'])
            ]
            
            if not matching_credits.empty:
                credit = matching_credits.iloc[0]
                days_diff = (credit['datetime'] - debit['datetime']).days
                
                refunds.append({
                    'debit_date': debit['datetime'],
                    'credit_date': credit['datetime'],
                    'beneficiary': debit['beneficiary'],
                    'amount': debit['debit_amount'],
                    'days_to_refund': days_diff,
                    'debit_narration': debit['narration'],
                    'credit_narration': credit['narration']
                })
            
            # Update progress
            if len(debits) > 100 and idx % 50 == 0:
                progress = idx / len(debits)
                progress_container.progress(progress, text=f"Checking refunds: {idx}/{len(debits)}")
        
        progress_container.empty()
        return refunds
    
    def _detect_duplicates(self, df):
        """Detect duplicate transactions"""
        duplicates = []
        debits = df[df['transaction_type'] == 'debit'].copy()
        debits = debits.sort_values('datetime')
        
        progress_container = st.empty()
        
        for i in range(len(debits)):
            txn1 = debits.iloc[i]
            
            # Look for similar transactions within time window
            for j in range(i + 1, len(debits)):
                txn2 = debits.iloc[j]
                
                # Check time difference
                time_diff = abs((txn2['datetime'] - txn1['datetime']).days)
                if time_diff > self.duplicate_days:
                    break  # No need to check further
                
                # Check amount similarity
                amount_diff = abs(txn2['debit_amount'] - txn1['debit_amount'])
                if amount_diff > self.amount_threshold:
                    continue
                
                # Check beneficiary similarity
                similarity = self._calculate_similarity(txn1['beneficiary'], txn2['beneficiary'])
                if similarity >= self.similarity_threshold:
                    duplicates.append([txn1.to_dict(), txn2.to_dict()])
            
            # Update progress
            if len(debits) > 100 and i % 25 == 0:
                progress = i / len(debits)
                progress_container.progress(progress, text=f"Checking duplicates: {i}/{len(debits)}")
                gc.collect()
        
        progress_container.empty()
        return duplicates
    
    def _calculate_similarity(self, str1, str2):
        """Calculate string similarity percentage"""
        if str1 == str2:
            return 100
        
        str1_set = set(str1.lower().split())
        str2_set = set(str2.lower().split())
        
        intersection = len(str1_set.intersection(str2_set))
        union = len(str1_set.union(str2_set))
        
        return (intersection / union * 100) if union > 0 else 0
    
    def _get_unmatched_debits(self, df, refunds):
        """Get debits that were never refunded"""
        refunded_dates = {r['debit_date'] for r in refunds}
        debits = df[df['transaction_type'] == 'debit']
        
        unmatched = []
        for _, debit in debits.iterrows():
            if debit['datetime'] not in refunded_dates:
                unmatched.append(debit.to_dict())
        
        return unmatched
    
    def _generate_summary(self, df, results):
        """Generate analysis summary"""
        total_debits = df[df['transaction_type'] == 'debit']['debit_amount'].sum()
        total_credits = df[df['transaction_type'] == 'credit']['credit_amount'].sum()
        total_refunded = sum(r['amount'] for r in results['refunds'])
        total_duplicates = sum(
            sum(t['debit_amount'] for t in group[1:]) 
            for group in results['duplicates']
        )
        
        return {
            'total_transactions': len(df),
            'total_debits': total_debits,
            'total_credits': total_credits,
            'total_refunded': total_refunded,
            'total_duplicates': total_duplicates,
            'estimated_loss': total_duplicates + (total_debits - total_credits - total_refunded),
            'refund_count': len(results['refunds']),
            'duplicate_groups': len(results['duplicates']),
            'unmatched_debits': len(results['unmatched_debits'])
        }
    
    def create_excel_report_streaming(self, all_transactions_df, results):
        """Create Excel report by streaming data to avoid memory issues"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Define formats
            header_format = workbook.add_format({
                'bold': True,
                'font_color': 'white',
                'bg_color': '#4472C4',
                'border': 1
            })
            
            green_format = workbook.add_format({
                'bg_color': '#90EE90',
                'border': 1
            })
            
            yellow_format = workbook.add_format({
                'bg_color': '#FFD700',
                'border': 1
            })
            
            currency_format = workbook.add_format({
                'num_format': '₦#,##0.00',
                'border': 1
            })
            
            # 1. All Transactions Sheet (chunked writing)
            st.info("Creating All Transactions sheet...")
            all_transactions_df.to_excel(writer, sheet_name='All Transactions', index=False)
            worksheet = writer.sheets['All Transactions']
            
            # Format headers
            for col_num, value in enumerate(all_transactions_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Format currency columns
            currency_cols = ['debit_amount', 'credit_amount', 'balance']
            for col_name in currency_cols:
                if col_name in all_transactions_df.columns:
                    col_idx = all_transactions_df.columns.get_loc(col_name)
                    worksheet.set_column(col_idx, col_idx, 15, currency_format)
            
            # 2. Refunded Transactions Sheet
            if results['refunds']:
                st.info("Creating Refunds sheet...")
                refunds_df = pd.DataFrame(results['refunds'])
                refunds_df.to_excel(writer, sheet_name='Refunded Transactions', index=False)
                
                worksheet = writer.sheets['Refunded Transactions']
                # Format headers
                for col_num, value in enumerate(refunds_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Apply green formatting to all data rows
                for row in range(1, len(refunds_df) + 1):
                    for col in range(len(refunds_df.columns)):
                        worksheet.set_row(row, cell_format=green_format)
            
            # 3. Suspected Duplicates Sheet
            if results['duplicates']:
                st.info("Creating Duplicates sheet...")
                duplicate_rows = []
                for group_id, dup_group in enumerate(results['duplicates'], 1):
                    for i, txn in enumerate(dup_group):
                        duplicate_rows.append({
                            'Group': f'GROUP_{group_id}',
                            'Date': txn['date'],
                            'Beneficiary': txn['beneficiary'],
                            'Amount': txn['debit_amount'],
                            'Narration': txn['narration'],
                            'Note': 'Original' if i == 0 else f'Suspected Duplicate {i}'
                        })
                
                duplicates_df = pd.DataFrame(duplicate_rows)
                duplicates_df.to_excel(writer, sheet_name='Suspected Duplicates', index=False)
                
                worksheet = writer.sheets['Suspected Duplicates']
                # Format headers
                for col_num, value in enumerate(duplicates_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Apply yellow formatting
                for row in range(1, len(duplicates_df) + 1):
                    for col in range(len(duplicates_df.columns)):
                        worksheet.set_row(row, cell_format=yellow_format)
            
            # 4. Summary Sheet
            st.info("Creating Summary sheet...")
            summary_data = [
                ['Metric', 'Value'],
                ['Total Transactions', results['summary']['total_transactions']],
                ['Total Debits', results['summary']['total_debits']],
                ['Total Credits', results['summary']['total_credits']],
                ['Total Refunded', results['summary']['total_refunded']],
                ['Estimated Duplicates Value', results['summary']['total_duplicates']],
                ['Estimated Loss', results['summary']['estimated_loss']],
                ['Refund Count', results['summary']['refund_count']],
                ['Duplicate Groups', results['summary']['duplicate_groups']],
                ['Unmatched Debits', results['summary']['unmatched_debits']]
            ]
            
            summary_worksheet = workbook.add_worksheet('Summary')
            for row_num, row_data in enumerate(summary_data):
                for col_num, cell_data in enumerate(row_data):
                    if row_num == 0:
                        summary_worksheet.write(row_num, col_num, cell_data, header_format)
                    else:
                        if col_num == 1 and isinstance(cell_data, (int, float)) and cell_data > 1000:
                            summary_worksheet.write(row_num, col_num, cell_data, currency_format)
                        else:
                            summary_worksheet.write(row_num, col_num, cell_data)
        
        output.seek(0)
        return output.getvalue()

def main():
    st.title("🏦 Table-Based Bank Transaction Analyzer")
    st.markdown("**Fast table extraction for structured PDF bank statements**")
    
    # Initialize session state
    if 'analysis_complete' not in st.session_state:
        st.session_state.analysis_complete = False
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'excel_data' not in st.session_state:
        st.session_state.excel_data = None
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Processing settings
        st.subheader("Processing Settings")
        batch_size = st.select_slider(
            "Pages per batch",
            options=[25, 50, 100, 200, 500],
            value=100,
            help="Process pages in batches to manage memory"
        )
        
        start_page = st.number_input(
            "Start from page",
            min_value=1,
            value=1,
            help="Skip initial pages if needed"
        ) - 1  # Convert to 0-based index
        
        # Analysis settings
        st.subheader("Analysis Parameters")
        duplicate_days = st.number_input("Duplicate Detection Window (days)", 1, 14, 3)
        amount_threshold = st.number_input("Amount Difference Threshold (₦)", 1, 10000, 1000)
        similarity_threshold = st.slider("Name Matching Threshold", 50, 100, 80)
    
    # File upload
    st.header("Upload Bank Statements")
    uploaded_files = st.file_uploader(
        "Choose PDF bank statement files",
        type=['pdf'],
        accept_multiple_files=True,
        help="Upload PDF files with tabular transaction data"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files uploaded")
        
        # Show file details
        total_size = sum(f.size for f in uploaded_files) / (1024 * 1024)
        st.info(f"Total size: {total_size:.1f} MB")
        
        if st.button("Analyze Transactions", type="primary"):
            with st.spinner("Processing with table-based extraction..."):
                analyzer = TableBasedAnalyzer(
                    duplicate_days=duplicate_days,
                    amount_threshold=amount_threshold,
                    similarity_threshold=similarity_threshold
                )
                
                all_transactions = []
                
                # Process each file
                file_progress = st.progress(0, text="Starting file processing...")
                
                for file_idx, uploaded_file in enumerate(uploaded_files):
                    file_progress.progress(
                        file_idx / len(uploaded_files),
                        text=f"Processing {uploaded_file.name} ({file_idx + 1}/{len(uploaded_files)})"
                    )
                    
                    # Extract transactions
                    transactions = analyzer.extract_transactions_from_pdf(
                        uploaded_file, start_page, batch_size
                    )
                    
                    if transactions:
                        # Add metadata
                        account_id = uploaded_file.name.split('.')[0]
                        for txn in transactions:
                            txn['account_id'] = account_id
                            txn['source_file'] = uploaded_file.name
                        
                        all_transactions.extend(transactions)
                        st.success(f"✅ {uploaded_file.name}: {len(transactions)} transactions")
                    else:
                        st.warning(f"⚠️ {uploaded_file.name}: No table data found")
                    
                    # Memory cleanup
                    del transactions
                    gc.collect()
                
                file_progress.progress(1.0, text="File processing complete!")
                
                if all_transactions:
                    # Convert to DataFrame
                    df = pd.DataFrame(all_transactions)
                    
                    # Analyze transactions
                    results = analyzer.analyze_transactions(df)
                    
                    # Generate Excel report
                    st.info("Generating Excel report...")
                    excel_data = analyzer.create_excel_report_streaming(df, results)
                    
                    # Store results
                    st.session_state.results = results
                    st.session_state.excel_data = excel_data
                    st.session_state.analysis_complete = True
                    
                    st.success("Analysis complete!")
                    st.rerun()
                else:
                    st.error("No transactions found in uploaded files")
    
    # Display results
    if st.session_state.analysis_complete and st.session_state.results:
        results = st.session_state.results
        
        st.header("Analysis Results")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Transactions", results['summary']['total_transactions'])
        with col2:
            st.metric("Total Debits", f"₦{results['summary']['total_debits']:,.2f}")
        with col3:
            st.metric("Refunds Found", f"{results['summary']['refund_count']}")
        with col4:
            st.metric("Duplicate Groups", f"{results['summary']['duplicate_groups']}")
        
        # Download Excel report
        if st.session_state.excel_data:
            st.download_button(
                label="📊 Download Complete Excel Report",
                data=st.session_state.excel_data,
                file_name="bank_transaction_analysis.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        # Quick preview
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Recent Refunds")
            if results['refunds']:
                for refund in results['refunds'][:5]:
                    st.write(f"**{refund['beneficiary']}** - ₦{refund['amount']:,.2f}")
                    st.write(f"Refunded after {refund['days_to_refund']} days")
                    st.divider()
            else:
                st.info("No refunds detected")
        
        with col2:
            st.subheader("Suspected Duplicates")
            if results['duplicates']:
                for i, dup_group in enumerate(results['duplicates'][:3], 1):
                    st.write(f"**Group {i}:**")
                    for j, txn in enumerate(dup_group):
                        label = "Original" if j == 0 else f"Duplicate {j}"
                        st.write(f"- {label}: ₦{txn['debit_amount']:,.2f} on {txn['date']}")
                    st.divider()
            else:
                st.info("No duplicates detected")

if __name__ == "__main__":
    main()