import streamlit as st
import pandas as pd
import pdfplumber
import os
import tempfile
import gc
from datetime import datetime, timedelta
from collections import defaultdict
import xlsxwriter
import io
import re
from fuzzywuzzy import fuzz

# Configure page
st.set_page_config(
    page_title="Bank Transaction Analyzer",
    page_icon="🏦",
    layout="wide"
)

class BankTransactionAnalyzer:
    """
    Comprehensive bank transaction analyzer with mandatory Account ID tracking
    for detecting refunds, duplicates, and financial losses
    """
    
    def __init__(self, duplicate_days=3, amount_threshold=1000, similarity_threshold=80):
        self.duplicate_days = duplicate_days
        self.amount_threshold = amount_threshold
        self.similarity_threshold = similarity_threshold
        self.temp_files = []
        self.processed_files = {}
    
    def extract_account_id_from_filename(self, filename):
        """
        Extract Account ID from PDF filename
        Example: GTBank_Jan.pdf -> GTB_Main
        """
        try:
            # Remove file extension
            base_name = os.path.splitext(filename)[0]
            
            # Extract bank name and create account ID
            if 'gtbank' in base_name.lower() or 'gtb' in base_name.lower():
                return "GTB_Main"
            elif 'access' in base_name.lower():
                return "ACC_Main"
            elif 'first' in base_name.lower():
                return "FBN_Main"
            elif 'zenith' in base_name.lower():
                return "ZEN_Main"
            elif 'uba' in base_name.lower():
                return "UBA_Main"
            elif 'union' in base_name.lower():
                return "UNI_Main"
            elif 'fidelity' in base_name.lower():
                return "FID_Main"
            elif 'sterling' in base_name.lower():
                return "STL_Main"
            else:
                # Generic account ID based on first 3 characters
                return f"{base_name[:3].upper()}_Main"
        except:
            return "UNK_Main"
    
    def extract_transactions_from_pdf(self, uploaded_file, start_page=0, batch_size=100):
        """
        Extract transactions from table-structured PDF pages with Account ID assignment
        """
        transactions = []
        
        # Extract Account ID from filename
        account_id = self.extract_account_id_from_filename(uploaded_file.name)
        
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
                                    transaction = self._parse_table_row(row, account_id, uploaded_file.name)
                                    if transaction:
                                        transactions.append(transaction)
                        
                        # Update progress
                        progress = (page_num - start_page + 1) / (end_page - start_page)
                        progress_container.progress(progress, 
                                                  text=f"Processing {uploaded_file.name} - Page {page_num + 1}/{total_pages}")
                        
                        # Memory cleanup every 10 pages
                        if page_num % 10 == 0:
                            gc.collect()
                            
                    except Exception as e:
                        st.warning(f"Could not process page {page_num + 1} in {uploaded_file.name}: {str(e)}")
                        continue
                
                progress_container.empty()
                
                # Store processed file info
                self.processed_files[uploaded_file.name] = {
                    'account_id': account_id,
                    'transactions_count': len(transactions),
                    'pages_processed': end_page - start_page
                }
                
        except Exception as e:
            st.error(f"Error processing PDF {uploaded_file.name}: {str(e)}")
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_file_path)
                if temp_file_path in self.temp_files:
                    self.temp_files.remove(temp_file_path)
            except:
                pass
        
        return transactions
    
    def _parse_table_row(self, row, account_id, source_file):
        """
        Parse a single table row into transaction dictionary with Account ID
        """
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
                'transaction_id': f"{account_id}_{dt.strftime('%Y%m%d_%H%M%S')}_{len(narration)}",
                'account_id': account_id,  # MANDATORY Account ID
                'source_file': source_file,
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
            # Remove common currency symbols and formatting
            clean_amount = str(amount_str).replace(',', '').replace('₦', '').replace('NGN', '')
            clean_amount = clean_amount.replace('N', '').replace('(', '').replace(')', '').strip()
            
            # Handle negative amounts
            if clean_amount.startswith('-'):
                return -float(clean_amount[1:]) if clean_amount[1:] else 0.0
            
            return float(clean_amount) if clean_amount else 0.0
        except:
            return 0.0
    
    def _parse_datetime(self, date_time_str):
        """Parse various datetime formats commonly found in bank statements"""
        formats = [
            '%d/%m/%Y, %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%d-%m-%Y, %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_time_str, fmt)
            except:
                continue
        
        # Try to extract date with regex as fallback
        date_pattern = r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})'
        time_pattern = r'(\d{1,2}):(\d{2}):(\d{2})'
        
        date_match = re.search(date_pattern, date_time_str)
        time_match = re.search(time_pattern, date_time_str)
        
        if date_match:
            day, month, year = date_match.groups()
            if time_match:
                hour, minute, second = time_match.groups()
                return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
            else:
                return datetime(int(year), int(month), int(day))
        
        raise ValueError(f"Cannot parse datetime: {date_time_str}")
    
    def _extract_beneficiary(self, narration):
        """Extract beneficiary name from narration with improved cleaning"""
        # Clean common prefixes and suffixes
        cleaners = [
            'TRANSFER TO:', 'PAYMENT TO:', 'TRF TO:', 'PAY:', 'TRANSFER', 'PAYMENT',
            'MOBILE TRANSFER TO:', 'WEB TRANSFER TO:', 'ATM TRANSFER TO:',
            'POS PURCHASE:', 'CARD PAYMENT:', 'ONLINE PAYMENT TO:'
        ]
        
        beneficiary = narration.upper()
        for cleaner in cleaners:
            beneficiary = beneficiary.replace(cleaner, '').strip()
        
        # Remove common separators and reference info
        beneficiary = beneficiary.split('/')[0].split('|')[0].split('REF:')[0]
        beneficiary = beneficiary.split('TXN:')[0].split('SESSION:')[0].strip()
        
        # Remove extra whitespace and limit length
        beneficiary = ' '.join(beneficiary.split())
        return beneficiary[:50]  # Limit length for cleaner display
    
    def analyze_transactions(self, all_transactions_df):
        """
        Comprehensive analysis with mandatory Account ID tracking
        """
        results = {
            'refunds': [],
            'duplicates': [],
            'unmatched_debits': [],
            'summary': {},
            'account_summary': {}
        }
        
        # Convert to DataFrame if not already
        if not isinstance(all_transactions_df, pd.DataFrame):
            df = pd.DataFrame(all_transactions_df)
        else:
            df = all_transactions_df
        
        if df.empty:
            return results
        
        # Sort by datetime for proper analysis
        df = df.sort_values('datetime').reset_index(drop=True)
        
        # Analyze refunds (within same account)
        st.info("🔍 Analyzing refunds within account boundaries...")
        results['refunds'] = self._detect_refunds_with_account_id(df)
        
        # Analyze duplicates (both within and across accounts)
        st.info("🔁 Detecting duplicate transactions with Account ID tracking...")
        results['duplicates'] = self._detect_duplicates_with_account_id(df)
        
        # Get unmatched debits
        st.info("❌ Identifying unmatched debits...")
        results['unmatched_debits'] = self._get_unmatched_debits_with_account_id(df, results['refunds'])
        
        # Generate comprehensive summary
        st.info("📊 Generating financial impact summary...")
        results['summary'] = self._generate_comprehensive_summary(df, results)
        results['account_summary'] = self._generate_account_summary(df, results)
        
        return results
    
    def _detect_refunds_with_account_id(self, df):
        """
        Detect refunded transactions within same account ID
        """
        refunds = []
        debits = df[df['transaction_type'] == 'debit'].copy()
        credits = df[df['transaction_type'] == 'credit'].copy()
        
        progress_container = st.empty()
        total_debits = len(debits)
        
        for idx, debit in debits.iterrows():
            # Look for matching credits after the debit WITHIN SAME ACCOUNT
            matching_credits = credits[
                (credits['datetime'] > debit['datetime']) &
                (credits['credit_amount'] == debit['debit_amount']) &
                (credits['account_id'] == debit['account_id'])  # Same account only
            ]
            
            if not matching_credits.empty:
                credit = matching_credits.iloc[0]
                days_diff = (credit['datetime'] - debit['datetime']).days
                hours_diff = (credit['datetime'] - debit['datetime']).total_seconds() / 3600
                
                refunds.append({
                    'account_id': debit['account_id'],  # MANDATORY Account ID
                    'debit_transaction_id': debit['transaction_id'],
                    'credit_transaction_id': credit['transaction_id'],
                    'debit_date': debit['datetime'],
                    'credit_date': credit['datetime'],
                    'beneficiary': debit['beneficiary'],
                    'amount': debit['debit_amount'],
                    'days_to_refund': days_diff,
                    'hours_to_refund': round(hours_diff, 2),
                    'debit_narration': debit['narration'],
                    'credit_narration': credit['narration'],
                    'debit_balance': debit['balance'],
                    'credit_balance': credit['balance'],
                    'source_file': debit['source_file']
                })
            
            # Update progress
            if total_debits > 100 and idx % 50 == 0:
                progress = idx / total_debits
                progress_container.progress(progress, text=f"Checking refunds: {idx}/{total_debits}")
        
        progress_container.empty()
        return refunds
    
    def _detect_duplicates_with_account_id(self, df):
        """
        Detect duplicate transactions with MANDATORY Account ID marking
        Works both within same account and across different accounts
        """
        duplicates = []
        debits = df[df['transaction_type'] == 'debit'].copy()
        debits = debits.sort_values('datetime').reset_index(drop=True)
        
        progress_container = st.empty()
        processed_pairs = set()
        
        for i in range(len(debits)):
            txn1 = debits.iloc[i]
            
            # Look for similar transactions within time window
            for j in range(i + 1, len(debits)):
                txn2 = debits.iloc[j]
                
                # Skip if already processed this pair
                pair_key = tuple(sorted([txn1['transaction_id'], txn2['transaction_id']]))
                if pair_key in processed_pairs:
                    continue
                
                # Check time difference
                time_diff = abs((txn2['datetime'] - txn1['datetime']).days)
                if time_diff > self.duplicate_days:
                    continue  # Outside time window
                
                # Check amount similarity
                amount_diff = abs(txn2['debit_amount'] - txn1['debit_amount'])
                if amount_diff > self.amount_threshold:
                    continue
                
                # Check beneficiary similarity
                similarity = fuzz.token_sort_ratio(txn1['beneficiary'], txn2['beneficiary'])
                if similarity >= self.similarity_threshold:
                    # Mark as processed
                    processed_pairs.add(pair_key)
                    
                    # Create duplicate group with MANDATORY Account ID marking
                    duplicate_group = {
                        'group_id': f"DUP_{len(duplicates) + 1}",
                        'transactions': [
                            {
                                'account_id': txn1['account_id'],  # MANDATORY Account ID
                                'transaction_id': txn1['transaction_id'],
                                'datetime': txn1['datetime'],
                                'date': txn1['date'],
                                'time': txn1['time'],
                                'beneficiary': txn1['beneficiary'],
                                'amount': txn1['debit_amount'],
                                'narration': txn1['narration'],
                                'balance': txn1['balance'],
                                'source_file': txn1['source_file'],
                                'is_original': True
                            },
                            {
                                'account_id': txn2['account_id'],  # MANDATORY Account ID
                                'transaction_id': txn2['transaction_id'],
                                'datetime': txn2['datetime'],
                                'date': txn2['date'],
                                'time': txn2['time'],
                                'beneficiary': txn2['beneficiary'],
                                'amount': txn2['debit_amount'],
                                'narration': txn2['narration'],
                                'balance': txn2['balance'],
                                'source_file': txn2['source_file'],
                                'is_original': False
                            }
                        ],
                        'similarity_score': similarity,
                        'amount_difference': amount_diff,
                        'time_difference_hours': abs((txn2['datetime'] - txn1['datetime']).total_seconds() / 3600),
                        'cross_account': txn1['account_id'] != txn2['account_id'],  # Mark if across accounts
                        'total_duplicate_amount': txn2['debit_amount']  # Amount of suspected duplicate
                    }
                    
                    duplicates.append(duplicate_group)
            
            # Update progress
            if len(debits) > 100 and i % 25 == 0:
                progress = i / len(debits)
                progress_container.progress(progress, text=f"Checking duplicates: {i}/{len(debits)}")
                gc.collect()
        
        progress_container.empty()
        return duplicates
    
    def _get_unmatched_debits_with_account_id(self, df, refunds):
        """
        Get debits that were never refunded with Account ID tracking
        """
        refunded_transaction_ids = {r['debit_transaction_id'] for r in refunds}
        debits = df[df['transaction_type'] == 'debit']
        
        unmatched = []
        for _, debit in debits.iterrows():
            if debit['transaction_id'] not in refunded_transaction_ids:
                unmatched_debit = debit.to_dict()
                unmatched_debit['account_id'] = debit['account_id']  # Ensure Account ID is included
                unmatched.append(unmatched_debit)
        
        return unmatched
    
    def _generate_comprehensive_summary(self, df, results):
        """
        Generate comprehensive analysis summary with Account ID breakdown
        """
        total_debits = df[df['transaction_type'] == 'debit']['debit_amount'].sum()
        total_credits = df[df['transaction_type'] == 'credit']['credit_amount'].sum()
        total_refunded = sum(r['amount'] for r in results['refunds'])
        
        # Calculate duplicate amounts
        total_duplicate_amount = sum(
            group['total_duplicate_amount'] for group in results['duplicates']
        )
        
        # Calculate potential losses
        unmatched_debit_amount = sum(
            d['debit_amount'] for d in results['unmatched_debits']
        )
        
        # Net loss calculation
        estimated_net_loss = total_duplicate_amount + (total_debits - total_credits - total_refunded)
        
        return {
            'total_transactions': len(df),
            'total_files_processed': len(self.processed_files),
            'total_debits': total_debits,
            'total_credits': total_credits,
            'total_refunded': total_refunded,
            'total_duplicate_amount': total_duplicate_amount,
            'unmatched_debit_amount': unmatched_debit_amount,
            'estimated_net_loss': max(0, estimated_net_loss),  # Don't show negative losses
            'refund_count': len(results['refunds']),
            'duplicate_groups': len(results['duplicates']),
            'unmatched_debits_count': len(results['unmatched_debits']),
            'cross_account_duplicates': sum(1 for d in results['duplicates'] if d['cross_account']),
            'same_account_duplicates': sum(1 for d in results['duplicates'] if not d['cross_account'])
        }
    
    def _generate_account_summary(self, df, results):
        """
        Generate per-account summary with financial impact
        """
        account_summary = {}
        
        for account_id in df['account_id'].unique():
            account_df = df[df['account_id'] == account_id]
            account_debits = account_df[account_df['transaction_type'] == 'debit']['debit_amount'].sum()
            account_credits = account_df[account_df['transaction_type'] == 'credit']['credit_amount'].sum()
            
            # Account-specific refunds
            account_refunds = [r for r in results['refunds'] if r['account_id'] == account_id]
            account_refunded = sum(r['amount'] for r in account_refunds)
            
            # Account-specific duplicates
            account_duplicates = [
                d for d in results['duplicates'] 
                if any(t['account_id'] == account_id for t in d['transactions'])
            ]
            
            account_summary[account_id] = {
                'total_transactions': len(account_df),
                'total_debits': account_debits,
                'total_credits': account_credits,
                'total_refunded': account_refunded,
                'refund_count': len(account_refunds),
                'duplicate_groups': len(account_duplicates),
                'source_files': list(account_df['source_file'].unique())
            }
        
        return account_summary
    
    def create_excel_report_with_account_tracking(self, all_transactions_df, results):
        """
        Create comprehensive Excel report with MANDATORY Account ID tracking
        """
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Define formats with Account ID emphasis
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#4F81BD',
                'font_color': 'white',
                'border': 1
            })
            
            # Green format for refunded transactions
            refund_format = workbook.add_format({
                'bg_color': '#C6EFCE',
                'border': 1
            })
            
            # Yellow format for suspected duplicates
            duplicate_format = workbook.add_format({
                'bg_color': '#FFEB9C',
                'border': 1
            })
            
            # Red format for unmatched debits
            unmatched_format = workbook.add_format({
                'bg_color': '#FFC7CE',
                'border': 1
            })
            
            # Account ID emphasis format
            account_id_format = workbook.add_format({
                'bold': True,
                'bg_color': '#E6E6FA',
                'border': 1
            })
            
            # Sheet 1: All Transactions with Account ID prominence
            df = pd.DataFrame(all_transactions_df) if not isinstance(all_transactions_df, pd.DataFrame) else all_transactions_df
            
            # Reorder columns to emphasize Account ID
            column_order = [
                'account_id', 'transaction_id', 'source_file', 'date', 'time', 
                'beneficiary', 'debit_amount', 'credit_amount', 'balance', 
                'transaction_type', 'narration', 'reference'
            ]
            
            df_ordered = df.reindex(columns=column_order)
            df_ordered.to_excel(writer, sheet_name='All Transactions', index=False)
            
            worksheet = writer.sheets['All Transactions']
            
            # Format headers
            for col_num, value in enumerate(df_ordered.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Emphasize Account ID column
            worksheet.set_column('A:A', 15, account_id_format)  # Account ID column
            worksheet.set_column('B:B', 20)  # Transaction ID
            worksheet.set_column('C:C', 20)  # Source File
            worksheet.set_column('F:F', 30)  # Beneficiary
            
            # Sheet 2: Refunded Transactions (Green highlighting)
            if results['refunds']:
                refunds_data = []
                for refund in results['refunds']:
                    refunds_data.append({
                        'Account ID': refund['account_id'],  # MANDATORY Account ID first
                        'Debit Date': refund['debit_date'].strftime('%d/%m/%Y %H:%M:%S'),
                        'Credit Date': refund['credit_date'].strftime('%d/%m/%Y %H:%M:%S'),
                        'Beneficiary': refund['beneficiary'],
                        'Amount': refund['amount'],
                        'Days to Refund': refund['days_to_refund'],
                        'Hours to Refund': refund['hours_to_refund'],
                        'Debit Balance': refund['debit_balance'],
                        'Credit Balance': refund['credit_balance'],
                        'Source File': refund['source_file'],
                        'Debit Narration': refund['debit_narration'],
                        'Credit Narration': refund['credit_narration']
                    })
                
                refunds_df = pd.DataFrame(refunds_data)
                refunds_df.to_excel(writer, sheet_name='Refunded Transactions', index=False)
                
                worksheet = writer.sheets['Refunded Transactions']
                
                # Format headers and apply green highlighting
                for col_num, value in enumerate(refunds_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Apply green formatting to all data rows
                for row in range(1, len(refunds_df) + 1):
                    for col in range(len(refunds_df.columns)):
                        if col == 0:  # Account ID column
                            worksheet.write(row, col, refunds_df.iloc[row-1, col], account_id_format)
                        else:
                            worksheet.write(row, col, refunds_df.iloc[row-1, col], refund_format)
                
                worksheet.set_column('A:A', 15)  # Account ID
                worksheet.set_column('D:D', 30)  # Beneficiary
            
            # Sheet 3: Unmatched Debits (Red highlighting)
            if results['unmatched_debits']:
                unmatched_data = []
                for debit in results['unmatched_debits']:
                    unmatched_data.append({
                        'Account ID': debit['account_id'],  # MANDATORY Account ID first
                        'Transaction ID': debit['transaction_id'],
                        'Date': debit['date'],
                        'Time': debit['time'],
                        'Beneficiary': debit['beneficiary'],
                        'Amount': debit['debit_amount'],
                        'Balance': debit['balance'],
                        'Source File': debit['source_file'],
                        'Narration': debit['narration'],
                        'Status': 'Not Refunded'
                    })
                
                unmatched_df = pd.DataFrame(unmatched_data)
                unmatched_df.to_excel(writer, sheet_name='Unmatched Debits', index=False)
                
                worksheet = writer.sheets['Unmatched Debits']
                
                # Format headers and apply red highlighting
                for col_num, value in enumerate(unmatched_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                for row in range(1, len(unmatched_df) + 1):
                    for col in range(len(unmatched_df.columns)):
                        if col == 0:  # Account ID column
                            worksheet.write(row, col, unmatched_df.iloc[row-1, col], account_id_format)
                        else:
                            worksheet.write(row, col, unmatched_df.iloc[row-1, col], unmatched_format)
                
                worksheet.set_column('A:A', 15)  # Account ID
                worksheet.set_column('E:E', 30)  # Beneficiary
            
            # Sheet 4: Suspected Duplicate Payments (Yellow highlighting)
            if results['duplicates']:
                duplicates_data = []
                for group in results['duplicates']:
                    for i, txn in enumerate(group['transactions']):
                        duplicates_data.append({
                            'Group ID': group['group_id'],
                            'Account ID': txn['account_id'],  # MANDATORY Account ID
                            'Transaction ID': txn['transaction_id'],
                            'Date': txn['date'],
                            'Time': txn['time'],
                            'Beneficiary': txn['beneficiary'],
                            'Amount': txn['amount'],
                            'Balance': txn['balance'],
                            'Source File': txn['source_file'],
                            'Is Original': 'Yes' if txn['is_original'] else 'No',
                            'Cross Account': 'Yes' if group['cross_account'] else 'No',
                            'Similarity Score': group['similarity_score'],
                            'Amount Difference': group['amount_difference'],
                            'Time Diff (Hours)': group['time_difference_hours'],
                            'Narration': txn['narration']
                        })
                
                duplicates_df = pd.DataFrame(duplicates_data)
                duplicates_df.to_excel(writer, sheet_name='Suspected Duplicates', index=False)
                
                worksheet = writer.sheets['Suspected Duplicates']
                
                # Format headers and apply yellow highlighting
                for col_num, value in enumerate(duplicates_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                for row in range(1, len(duplicates_df) + 1):
                    for col in range(len(duplicates_df.columns)):
                        if col == 1:  # Account ID column
                            worksheet.write(row, col, duplicates_df.iloc[row-1, col], account_id_format)
                        else:
                            worksheet.write(row, col, duplicates_df.iloc[row-1, col], duplicate_format)
                
                worksheet.set_column('A:A', 12)  # Group ID
                worksheet.set_column('B:B', 15)  # Account ID
                worksheet.set_column('F:F', 30)  # Beneficiary
            
            # Sheet 5: Summary of Potential Losses
            summary_data = [
                ['Metric', 'Value'],
                ['Total Files Processed', results['summary']['total_files_processed']],
                ['Total Transactions', results['summary']['total_transactions']],
                ['Total Debits', f"₦{results['summary']['total_debits']:,.2f}"],
                ['Total Credits', f"₦{results['summary']['total_credits']:,.2f}"],
                ['Total Refunded', f"₦{results['summary']['total_refunded']:,.2f}"],
                ['Total Duplicate Amount', f"₦{results['summary']['total_duplicate_amount']:,.2f}"],
                ['Unmatched Debits Amount', f"₦{results['summary']['unmatched_debit_amount']:,.2f}"],
                ['Estimated Net Loss', f"₦{results['summary']['estimated_net_loss']:,.2f}"],
                ['', ''],
                ['Refund Transactions', results['summary']['refund_count']],
                ['Duplicate Groups', results['summary']['duplicate_groups']],
                ['Same Account Duplicates', results['summary']['same_account_duplicates']],
                ['Cross Account Duplicates', results['summary']['cross_account_duplicates']],
                ['Unmatched Debits', results['summary']['unmatched_debits_count']]
            ]
            
            summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            worksheet = writer.sheets['Summary']
            
            # Format summary sheet
            for col_num, value in enumerate(summary_data[0]):
                worksheet.write(0, col_num, value, header_format)
            
            worksheet.set_column('A:A', 25)
            worksheet.set_column('B:B', 20)
            
            # Sheet 6: Account Breakdown
            if results['account_summary']:
                account_data = []
                for account_id, summary in results['account_summary'].items():
                    account_data.append({
                        'Account ID': account_id,
                        'Total Transactions': summary['total_transactions'],
                        'Total Debits': summary['total_debits'],
                        'Total Credits': summary['total_credits'],
                        'Total Refunded': summary['total_refunded'],
                        'Refund Count': summary['refund_count'],
                        'Duplicate Groups': summary['duplicate_groups'],
                        'Source Files': ', '.join(summary['source_files'])
                    })
                
                account_df = pd.DataFrame(account_data)
                account_df.to_excel(writer, sheet_name='Account Summary', index=False)
                
                worksheet = writer.sheets['Account Summary']
                
                for col_num, value in enumerate(account_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # Emphasize Account ID column
                for row in range(1, len(account_df) + 1):
                    worksheet.write(row, 0, account_df.iloc[row-1, 0], account_id_format)
                
                worksheet.set_column('A:A', 15)  # Account ID
                worksheet.set_column('H:H', 40)  # Source Files
        
        output.seek(0)
        return output

def main():
    """
    Main Streamlit application with comprehensive bank transaction analysis
    """
    st.title("🏦 Bank Transaction Analyzer")
    st.subtitle("Comprehensive PDF Statement Analysis with Account ID Tracking")
    
    st.markdown("""
    **Key Features:**
    - ✅ **Account ID Assignment**: Automatic account identification from PDF filenames
    - 🔍 **Refund Detection**: Match debits with subsequent credits within same account
    - 🔁 **Duplicate Detection**: Identify potential duplicate payments with account tracking
    - ❌ **Loss Identification**: Track unmatched debits and financial impact
    - 📊 **Excel Reporting**: Multi-sheet reports with color coding and account summaries
    """)
    
    # Initialize analyzer
    if 'analyzer' not in st.session_state:
        st.session_state.analyzer = BankTransactionAnalyzer()
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Performance settings
    st.sidebar.subheader("Performance Settings")
    batch_size = st.sidebar.selectbox(
        "Batch Processing Size",
        [50, 100, 200, 500, 1000],
        index=1,
        help="Smaller batches use less memory but process slower"
    )
    
    # Analysis parameters
    st.sidebar.subheader("Analysis Parameters")
    duplicate_days = st.sidebar.slider(
        "Duplicate Detection Window (days)",
        min_value=1,
        max_value=14,
        value=3,
        help="Number of days to look for duplicate transactions"
    )
    
    amount_threshold = st.sidebar.slider(
        "Amount Difference Threshold (₦)",
        min_value=1,
        max_value=10000,
        value=1000,
        help="Maximum amount difference to consider as duplicate"
    )
    
    similarity_threshold = st.sidebar.slider(
        "Name Matching Threshold (%)",
        min_value=50,
        max_value=100,
        value=80,
        help="Similarity percentage for beneficiary name matching"
    )
    
    # Update analyzer parameters
    st.session_state.analyzer.duplicate_days = duplicate_days
    st.session_state.analyzer.amount_threshold = amount_threshold
    st.session_state.analyzer.similarity_threshold = similarity_threshold
    
    # File upload
    st.header("📁 Upload Bank Statements")
    uploaded_files = st.file_uploader(
        "Choose PDF bank statement files",
        type="pdf",
        accept_multiple_files=True,
        help="Upload multiple PDF files from the same or different bank accounts"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files uploaded successfully")
        
        # Display file information with predicted Account IDs
        st.subheader("📋 File Information & Account ID Assignment")
        file_info = []
        for file in uploaded_files:
            account_id = st.session_state.analyzer.extract_account_id_from_filename(file.name)
            file_info.append({
                'Filename': file.name,
                'Size (MB)': round(file.size / (1024 * 1024), 2),
                'Account ID': account_id
            })
        
        file_df = pd.DataFrame(file_info)
        st.dataframe(file_df, use_container_width=True)
        
        # Process files button
        if st.button("🔄 Analyze Transactions", type="primary"):
            with st.spinner("Processing PDF files and analyzing transactions..."):
                
                # Extract transactions from all files
                all_transactions = []
                
                st.info("📖 Extracting transactions from PDF files...")
                for file in uploaded_files:
                    try:
                        transactions = st.session_state.analyzer.extract_transactions_from_pdf(
                            file, 
                            batch_size=batch_size
                        )
                        all_transactions.extend(transactions)
                        st.success(f"✅ Processed {file.name}: {len(transactions)} transactions extracted")
                    except Exception as e:
                        st.error(f"❌ Error processing {file.name}: {str(e)}")
                
                if all_transactions:
                    # Convert to DataFrame
                    df = pd.DataFrame(all_transactions)
                    
                    # Store in session state
                    st.session_state.all_transactions = df
                    st.session_state.processed_files_info = st.session_state.analyzer.processed_files
                    
                    # Perform analysis
                    st.info("🔍 Performing comprehensive analysis...")
                    results = st.session_state.analyzer.analyze_transactions(df)
                    st.session_state.analysis_results = results
                    
                    st.success("✅ Analysis completed successfully!")
                    st.rerun()
                else:
                    st.error("❌ No transactions were extracted from the uploaded files.")
    
    # Display results if available
    if 'analysis_results' in st.session_state and 'all_transactions' in st.session_state:
        results = st.session_state.analysis_results
        df = st.session_state.all_transactions
        
        # Summary metrics
        st.header("📊 Analysis Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Transactions",
                f"{results['summary']['total_transactions']:,}",
                delta=f"{results['summary']['total_files_processed']} files"
            )
        
        with col2:
            st.metric(
                "Total Debits",
                f"₦{results['summary']['total_debits']:,.2f}"
            )
        
        with col3:
            st.metric(
                "Total Refunded",
                f"₦{results['summary']['total_refunded']:,.2f}",
                delta=f"{results['summary']['refund_count']} refunds"
            )
        
        with col4:
            st.metric(
                "Estimated Net Loss",
                f"₦{results['summary']['estimated_net_loss']:,.2f}",
                delta=f"{results['summary']['duplicate_groups']} duplicate groups"
            )
        
        # Account Summary
        st.subheader("🏦 Account Summary")
        if results['account_summary']:
            account_summary_data = []
            for account_id, summary in results['account_summary'].items():
                account_summary_data.append({
                    'Account ID': account_id,
                    'Transactions': summary['total_transactions'],
                    'Debits': f"₦{summary['total_debits']:,.2f}",
                    'Credits': f"₦{summary['total_credits']:,.2f}",
                    'Refunds': f"₦{summary['total_refunded']:,.2f}",
                    'Duplicate Groups': summary['duplicate_groups'],
                    'Files': len(summary['source_files'])
                })
            
            account_summary_df = pd.DataFrame(account_summary_data)
            st.dataframe(account_summary_df, use_container_width=True)
        
        # Detailed results tabs
        tab1, tab2, tab3, tab4 = st.tabs(["🟢 Refunds", "🟡 Duplicates", "🔴 Unmatched Debits", "📋 All Transactions"])
        
        with tab1:
            st.subheader("🟢 Refunded Transactions")
            if results['refunds']:
                refunds_display = []
                for refund in results['refunds']:
                    refunds_display.append({
                        'Account ID': refund['account_id'],
                        'Debit Date': refund['debit_date'].strftime('%d/%m/%Y %H:%M'),
                        'Credit Date': refund['credit_date'].strftime('%d/%m/%Y %H:%M'),
                        'Beneficiary': refund['beneficiary'],
                        'Amount': f"₦{refund['amount']:,.2f}",
                        'Days to Refund': refund['days_to_refund'],
                        'Source File': refund['source_file']
                    })
                
                refunds_df = pd.DataFrame(refunds_display)
                st.dataframe(refunds_df, use_container_width=True)
            else:
                st.info("No refunded transactions found.")
        
        with tab2:
            st.subheader("🟡 Suspected Duplicate Payments")
            if results['duplicates']:
                st.info(f"Found {len(results['duplicates'])} duplicate groups")
                
                for group in results['duplicates']:
                    with st.expander(f"Duplicate Group: {group['group_id']} ({'Cross-Account' if group['cross_account'] else 'Same Account'})"):
                        st.write(f"**Similarity Score:** {group['similarity_score']}%")
                        st.write(f"**Amount Difference:** ₦{group['amount_difference']:,.2f}")
                        st.write(f"**Time Difference:** {group['time_difference_hours']:.2f} hours")
                        
                        group_data = []
                        for txn in group['transactions']:
                            group_data.append({
                                'Account ID': txn['account_id'],
                                'Date': txn['date'],
                                'Time': txn['time'],
                                'Beneficiary': txn['beneficiary'],
                                'Amount': f"₦{txn['amount']:,.2f}",
                                'Balance': f"₦{txn['balance']:,.2f}",
                                'Original': 'Yes' if txn['is_original'] else 'No',
                                'Source File': txn['source_file']
                            })
                        
                        group_df = pd.DataFrame(group_data)
                        st.dataframe(group_df, use_container_width=True)
            else:
                st.info("No duplicate transactions found.")
        
        with tab3:
            st.subheader("🔴 Unmatched Debits")
            if results['unmatched_debits']:
                st.warning(f"Found {len(results['unmatched_debits'])} unmatched debits totaling ₦{results['summary']['unmatched_debit_amount']:,.2f}")
                
                unmatched_display = []
                for debit in results['unmatched_debits'][:100]:  # Limit display for performance
                    unmatched_display.append({
                        'Account ID': debit['account_id'],
                        'Date': debit['date'],
                        'Time': debit['time'],
                        'Beneficiary': debit['beneficiary'],
                        'Amount': f"₦{debit['debit_amount']:,.2f}",
                        'Balance': f"₦{debit['balance']:,.2f}",
                        'Source File': debit['source_file']
                    })
                
                unmatched_df = pd.DataFrame(unmatched_display)
                st.dataframe(unmatched_df, use_container_width=True)
                
                if len(results['unmatched_debits']) > 100:
                    st.info(f"Showing first 100 of {len(results['unmatched_debits'])} unmatched debits. Download Excel report for complete data.")
            else:
                st.success("All debits have been matched with credits or identified as duplicates.")
        
        with tab4:
            st.subheader("📋 All Transactions")
            st.info(f"Total transactions: {len(df)}")
            
            # Display sample of transactions
            display_df = df[['account_id', 'date', 'time', 'beneficiary', 'debit_amount', 'credit_amount', 'balance', 'transaction_type', 'source_file']].head(100)
            st.dataframe(display_df, use_container_width=True)
            
            if len(df) > 100:
                st.info("Showing first 100 transactions. Download Excel report for complete data.")
        
        # Download Excel report
        st.header("📥 Download Report")
        if st.button("Generate Excel Report", type="primary"):
            with st.spinner("Generating comprehensive Excel report..."):
                try:
                    excel_buffer = st.session_state.analyzer.create_excel_report_with_account_tracking(df, results)
                    
                    st.download_button(
                        label="📊 Download Excel Report",
                        data=excel_buffer.getvalue(),
                        file_name=f"Bank_Transaction_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    st.success("✅ Excel report generated successfully!")
                except Exception as e:
                    st.error(f"❌ Error generating Excel report: {str(e)}")

if __name__ == "__main__":
    main()
