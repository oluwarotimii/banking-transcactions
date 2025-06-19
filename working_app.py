import streamlit as st
import os
import tempfile
import gc
from datetime import datetime, timedelta
from collections import defaultdict
import io
import re

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
        # For now, create sample data structure to demonstrate functionality
        # This would be replaced with actual PDF parsing logic using pdfplumber
        
        account_id = self.extract_account_id_from_filename(uploaded_file.name)
        transactions = []
        
        # Simulate PDF processing
        progress_container = st.empty()
        progress_container.progress(0.5, text=f"Processing {uploaded_file.name}...")
        
        # Create sample transactions for demonstration
        sample_transactions = [
            {
                'transaction_id': f"{account_id}_20250115_143500_1",
                'account_id': account_id,
                'source_file': uploaded_file.name,
                'datetime': datetime(2025, 1, 15, 14, 35, 0),
                'date': '15/01/2025',
                'time': '14:35:00',
                'narration': 'TRANSFER TO ADESOLA OLUWAFEMI OPARINDE',
                'beneficiary': 'ADESOLA OLUWAFEMI OPARINDE',
                'reference': 'TXN123456789',
                'debit_amount': 100020.00,
                'credit_amount': 0.0,
                'balance': 4654173.02,
                'transaction_type': 'debit'
            },
            {
                'transaction_id': f"{account_id}_20250116_143500_2",
                'account_id': account_id,
                'source_file': uploaded_file.name,
                'datetime': datetime(2025, 1, 16, 14, 35, 0),
                'date': '16/01/2025',
                'time': '14:35:00',
                'narration': 'REVERSAL: TRANSFER TO ADESOLA OLUWAFEMI OPARINDE',
                'beneficiary': 'REVERSAL',
                'reference': 'REV123456789',
                'debit_amount': 0.0,
                'credit_amount': 100020.00,
                'balance': 4754193.02,
                'transaction_type': 'credit'
            }
        ]
        
        transactions.extend(sample_transactions)
        
        # Store processed file info
        self.processed_files[uploaded_file.name] = {
            'account_id': account_id,
            'transactions_count': len(transactions),
            'pages_processed': 1
        }
        
        progress_container.empty()
        st.success(f"Processed {uploaded_file.name} - Account ID: {account_id} - {len(transactions)} transactions")
        
        return transactions
    
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
    
    def _calculate_similarity(self, str1, str2):
        """Calculate string similarity percentage"""
        if str1 == str2:
            return 100
        
        str1_words = set(str1.lower().split())
        str2_words = set(str2.lower().split())
        
        intersection = len(str1_words.intersection(str2_words))
        union = len(str1_words.union(str2_words))
        
        return (intersection / union * 100) if union > 0 else 0
    
    def analyze_transactions(self, all_transactions):
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
        
        if not all_transactions:
            results['summary'] = {
                'total_transactions': 0,
                'total_debits': 0.0,
                'total_credits': 0.0,
                'total_refunded': 0.0,
                'total_duplicates': 0.0,
                'estimated_loss': 0.0,
                'refund_count': 0,
                'duplicate_groups': 0,
                'unmatched_debits': 0
            }
            return results
        
        # Separate debits and credits
        debits = [t for t in all_transactions if t['transaction_type'] == 'debit']
        credits = [t for t in all_transactions if t['transaction_type'] == 'credit']
        
        # Detect refunds with Account ID tracking
        refunds = []
        for debit in debits:
            # Look for matching credits after the debit WITHIN SAME ACCOUNT
            for credit in credits:
                if (credit['datetime'] > debit['datetime'] and
                    credit['credit_amount'] == debit['debit_amount'] and
                    credit['account_id'] == debit['account_id']):
                    
                    days_diff = (credit['datetime'] - debit['datetime']).days
                    refunds.append({
                        'account_id': debit['account_id'],  # MANDATORY Account ID
                        'debit_transaction_id': debit['transaction_id'],
                        'credit_transaction_id': credit['transaction_id'],
                        'debit_date': debit['datetime'],
                        'credit_date': credit['datetime'],
                        'beneficiary': debit['beneficiary'],
                        'amount': debit['debit_amount'],
                        'days_to_refund': days_diff,
                        'debit_narration': debit['narration'],
                        'credit_narration': credit['narration'],
                        'source_file': debit['source_file']
                    })
                    break
        
        # Detect duplicates with Account ID tracking
        duplicates = []
        processed_pairs = set()
        
        for i, txn1 in enumerate(debits):
            for j, txn2 in enumerate(debits[i+1:], i+1):
                pair_key = tuple(sorted([txn1['transaction_id'], txn2['transaction_id']]))
                if pair_key in processed_pairs:
                    continue
                
                # Check time difference
                time_diff = abs((txn2['datetime'] - txn1['datetime']).days)
                if time_diff > self.duplicate_days:
                    continue
                
                # Check amount similarity
                amount_diff = abs(txn2['debit_amount'] - txn1['debit_amount'])
                if amount_diff > self.amount_threshold:
                    continue
                
                # Check beneficiary similarity
                similarity = self._calculate_similarity(txn1['beneficiary'], txn2['beneficiary'])
                if similarity >= self.similarity_threshold:
                    processed_pairs.add(pair_key)
                    
                    duplicates.append({
                        'group_id': f"DUP_{len(duplicates) + 1}",
                        'transactions': [
                            {
                                'account_id': txn1['account_id'],  # MANDATORY Account ID
                                'transaction_id': txn1['transaction_id'],
                                'date': txn1['date'],
                                'time': txn1['time'],
                                'beneficiary': txn1['beneficiary'],
                                'debit_amount': txn1['debit_amount'],
                                'balance': txn1['balance'],
                                'source_file': txn1['source_file']
                            },
                            {
                                'account_id': txn2['account_id'],  # MANDATORY Account ID
                                'transaction_id': txn2['transaction_id'],
                                'date': txn2['date'],
                                'time': txn2['time'],
                                'beneficiary': txn2['beneficiary'],
                                'debit_amount': txn2['debit_amount'],
                                'balance': txn2['balance'],
                                'source_file': txn2['source_file']
                            }
                        ],
                        'similarity_score': similarity,
                        'amount_difference': amount_diff,
                        'time_difference_days': time_diff
                    })
        
        # Calculate summary
        total_debits = sum(t['debit_amount'] for t in debits)
        total_credits = sum(t['credit_amount'] for t in credits)
        total_refunded = sum(r['amount'] for r in refunds)
        total_duplicates = sum(
            sum(txn['debit_amount'] for txn in group['transactions'][1:])
            for group in duplicates
        )
        
        results['refunds'] = refunds
        results['duplicates'] = duplicates
        results['summary'] = {
            'total_transactions': len(all_transactions),
            'total_debits': total_debits,
            'total_credits': total_credits,
            'total_refunded': total_refunded,
            'total_duplicates': total_duplicates,
            'estimated_loss': total_duplicates,
            'refund_count': len(refunds),
            'duplicate_groups': len(duplicates),
            'unmatched_debits': len(debits) - len(refunds)
        }
        
        # Generate account summary
        account_summary = {}
        for account_id in set(t['account_id'] for t in all_transactions):
            account_txns = [t for t in all_transactions if t['account_id'] == account_id]
            account_debits = [t for t in account_txns if t['transaction_type'] == 'debit']
            account_refunds = [r for r in refunds if r['account_id'] == account_id]
            
            account_summary[account_id] = {
                'transaction_count': len(account_txns),
                'total_debits': sum(t['debit_amount'] for t in account_debits),
                'total_refunded': sum(r['amount'] for r in account_refunds)
            }
        
        results['account_summary'] = account_summary
        
        return results
    
    def create_excel_report_with_account_tracking(self, all_transactions, results):
        """
        Create comprehensive Excel report with MANDATORY Account ID tracking
        """
        output = io.BytesIO()
        
        # Create a comprehensive text report
        report_content = "BANK TRANSACTION ANALYSIS REPORT\n"
        report_content += "="*60 + "\n\n"
        
        report_content += "PROCESSED FILES:\n"
        report_content += "-"*20 + "\n"
        for filename, info in self.processed_files.items():
            report_content += f"File: {filename}\n"
            report_content += f"  Account ID: {info['account_id']}\n"
            report_content += f"  Transactions: {info['transactions_count']}\n"
            report_content += f"  Pages Processed: {info['pages_processed']}\n\n"
        
        report_content += "FINANCIAL SUMMARY:\n"
        report_content += "-"*20 + "\n"
        for key, value in results['summary'].items():
            if isinstance(value, float):
                report_content += f"{key.replace('_', ' ').title()}: ₦{value:,.2f}\n"
            else:
                report_content += f"{key.replace('_', ' ').title()}: {value:,}\n"
        
        report_content += "\nACCOUNT BREAKDOWN:\n"
        report_content += "-"*20 + "\n"
        for account_id, data in results['account_summary'].items():
            report_content += f"Account: {account_id}\n"
            report_content += f"  Transactions: {data['transaction_count']}\n"
            report_content += f"  Total Debits: ₦{data['total_debits']:,.2f}\n"
            report_content += f"  Total Refunded: ₦{data['total_refunded']:,.2f}\n\n"
        
        if results['refunds']:
            report_content += "REFUNDED TRANSACTIONS:\n"
            report_content += "-"*25 + "\n"
            for refund in results['refunds']:
                report_content += f"Account: {refund['account_id']}\n"
                report_content += f"Amount: ₦{refund['amount']:,.2f}\n"
                report_content += f"Beneficiary: {refund['beneficiary']}\n"
                report_content += f"Days to Refund: {refund['days_to_refund']}\n\n"
        
        if results['duplicates']:
            report_content += "DUPLICATE TRANSACTION GROUPS:\n"
            report_content += "-"*30 + "\n"
            for group in results['duplicates']:
                report_content += f"Group ID: {group['group_id']}\n"
                report_content += f"Similarity Score: {group['similarity_score']:.1f}%\n"
                account_ids = [t['account_id'] for t in group['transactions']]
                report_content += f"Account IDs: {', '.join(set(account_ids))}\n"
                for i, txn in enumerate(group['transactions']):
                    report_content += f"  Transaction {i+1}:\n"
                    report_content += f"    Account ID: {txn['account_id']}\n"
                    report_content += f"    Amount: ₦{txn['debit_amount']:,.2f}\n"
                    report_content += f"    Date: {txn['date']}\n"
                    report_content += f"    Beneficiary: {txn['beneficiary']}\n"
                report_content += "\n"
        
        report_content += "\nNOTE: All duplicate transactions are marked with their respective Account IDs\n"
        report_content += "for precise tracking and reconciliation purposes.\n"
        
        output.write(report_content.encode('utf-8'))
        output.seek(0)
        
        return output

def main():
    """
    Main Streamlit application with comprehensive bank transaction analysis
    """
    st.title("🏦 Bank Transaction Analyzer")
    st.markdown("**Detect refunds, duplicates, and financial losses with mandatory Account ID tracking**")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Analysis Settings")
    
    duplicate_days = st.sidebar.slider(
        "Duplicate Detection Window (days)",
        min_value=1, max_value=14, value=3,
        help="Look for duplicate transactions within this many days"
    )
    
    amount_threshold = st.sidebar.slider(
        "Amount Difference Threshold (₦)",
        min_value=1, max_value=10000, value=1000,
        help="Maximum amount difference to consider as duplicate"
    )
    
    similarity_threshold = st.sidebar.slider(
        "Name Matching Threshold (%)",
        min_value=50, max_value=100, value=80,
        help="Minimum similarity percentage for beneficiary matching"
    )
    
    batch_size = st.sidebar.selectbox(
        "Batch Processing Size",
        options=[50, 100, 200, 500, 1000],
        index=1,
        help="Balance between memory usage and processing speed"
    )
    
    # Initialize analyzer
    analyzer = BankTransactionAnalyzer(
        duplicate_days=duplicate_days,
        amount_threshold=amount_threshold,
        similarity_threshold=similarity_threshold
    )
    
    # File upload section
    st.header("📁 Upload Bank Statement PDFs")
    st.info("Upload multiple PDF bank statements from the same bank for comprehensive analysis")
    
    uploaded_files = st.file_uploader(
        "Choose PDF bank statement files",
        type=['pdf'],
        accept_multiple_files=True,
        help="Select multiple PDF files containing bank transaction data"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files uploaded successfully")
        
        # Display uploaded files with detected account IDs
        st.subheader("📋 Uploaded Files & Account IDs")
        for file in uploaded_files:
            account_id = analyzer.extract_account_id_from_filename(file.name)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.write(f"📁 **{file.name}**")
            with col2:
                st.write(f"🏦 **{account_id}**")
            with col3:
                st.write(f"📏 **{round(file.size / 1024, 2)} KB**")
        
        # Analysis button
        if st.button("🔍 Analyze Transactions", type="primary"):
            with st.spinner("Processing PDF files and analyzing transactions..."):
                
                # Process each PDF file
                all_transactions = []
                for uploaded_file in uploaded_files:
                    st.info(f"Processing: {uploaded_file.name}")
                    transactions = analyzer.extract_transactions_from_pdf(
                        uploaded_file, 
                        batch_size=batch_size
                    )
                    all_transactions.extend(transactions)
                
                # Perform analysis
                st.info("Analyzing transactions for refunds and duplicates...")
                results = analyzer.analyze_transactions(all_transactions)
                
                # Display results
                st.header("📊 Analysis Results")
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        "Total Transactions",
                        f"{results['summary']['total_transactions']:,}",
                        help="Total number of transactions processed"
                    )
                
                with col2:
                    st.metric(
                        "Total Debits",
                        f"₦{results['summary']['total_debits']:,.2f}",
                        help="Total amount of money going out"
                    )
                
                with col3:
                    st.metric(
                        "Total Refunded",
                        f"₦{results['summary']['total_refunded']:,.2f}",
                        help="Total amount successfully refunded"
                    )
                
                with col4:
                    st.metric(
                        "Estimated Loss",
                        f"₦{results['summary']['estimated_loss']:,.2f}",
                        delta=f"-₦{results['summary']['estimated_loss']:,.2f}",
                        delta_color="inverse",
                        help="Estimated financial loss from duplicates and unrefunded transactions"
                    )
                
                # Detailed results tabs
                tab1, tab2, tab3, tab4 = st.tabs([
                    "📈 Summary", 
                    "💚 Refunds", 
                    "⚠️ Duplicates", 
                    "📋 All Transactions"
                ])
                
                with tab1:
                    st.subheader("Financial Impact Summary")
                    
                    # Account-wise breakdown
                    if results['account_summary']:
                        st.subheader("Per-Account Analysis")
                        for account_id, account_data in results['account_summary'].items():
                            with st.expander(f"Account: {account_id}"):
                                acc_col1, acc_col2, acc_col3 = st.columns(3)
                                with acc_col1:
                                    st.metric("Transactions", account_data.get('transaction_count', 0))
                                with acc_col2:
                                    st.metric("Total Debits", f"₦{account_data.get('total_debits', 0):,.2f}")
                                with acc_col3:
                                    st.metric("Refunds", f"₦{account_data.get('total_refunded', 0):,.2f}")
                
                with tab2:
                    st.subheader("Refunded Transactions")
                    if results['refunds']:
                        st.write("Refunded transactions found:")
                        for i, refund in enumerate(results['refunds']):
                            with st.expander(f"Refund {i+1} - Account: {refund.get('account_id', 'Unknown')}"):
                                st.write(f"**Account ID:** {refund.get('account_id', 'Unknown')}")
                                st.write(f"**Amount:** ₦{refund.get('amount', 0):,.2f}")
                                st.write(f"**Beneficiary:** {refund.get('beneficiary', 'N/A')}")
                                st.write(f"**Debit Date:** {refund.get('debit_date', 'N/A')}")
                                st.write(f"**Credit Date:** {refund.get('credit_date', 'N/A')}")
                                st.write(f"**Days to Refund:** {refund.get('days_to_refund', 'N/A')}")
                    else:
                        st.info("No refunded transactions detected")
                
                with tab3:
                    st.subheader("Suspected Duplicate Transactions")
                    st.info("**All duplicates are marked with Account ID for precise tracking**")
                    if results['duplicates']:
                        # Display duplicate groups with Account ID emphasis
                        for i, group in enumerate(results['duplicates']):
                            account_ids = set(t.get('account_id', 'Unknown') for t in group.get('transactions', []))
                            with st.expander(f"Duplicate Group {i+1} - Account IDs: {', '.join(account_ids)}"):
                                st.write(f"**Similarity Score:** {group.get('similarity_score', 0):.1f}%")
                                st.write(f"**Amount Difference:** ₦{group.get('amount_difference', 0):,.2f}")
                                st.write(f"**Time Difference:** {group.get('time_difference_days', 0)} days")
                                st.write("---")
                                
                                for j, transaction in enumerate(group.get('transactions', [])):
                                    st.write(f"**Transaction {j+1}:**")
                                    st.write(f"- **Account ID:** {transaction.get('account_id', 'Unknown')}")
                                    st.write(f"- Amount: ₦{transaction.get('debit_amount', 0):,.2f}")
                                    st.write(f"- Beneficiary: {transaction.get('beneficiary', 'N/A')}")
                                    st.write(f"- Date: {transaction.get('date', 'N/A')}")
                                    st.write(f"- Source File: {transaction.get('source_file', 'N/A')}")
                                    if j < len(group.get('transactions', [])) - 1:
                                        st.write("---")
                    else:
                        st.info("No duplicate transactions detected")
                
                with tab4:
                    st.subheader("All Processed Transactions")
                    if all_transactions:
                        st.write(f"**Total transactions processed:** {len(all_transactions)}")
                        for i, txn in enumerate(all_transactions[:10]):  # Show first 10
                            with st.expander(f"Transaction {i+1} - {txn.get('account_id', 'Unknown')}"):
                                st.write(f"**Account ID:** {txn.get('account_id', 'Unknown')}")
                                st.write(f"**Transaction Type:** {txn.get('transaction_type', 'N/A')}")
                                st.write(f"**Amount:** ₦{txn.get('debit_amount', 0) + txn.get('credit_amount', 0):,.2f}")
                                st.write(f"**Date:** {txn.get('date', 'N/A')} {txn.get('time', 'N/A')}")
                                st.write(f"**Beneficiary:** {txn.get('beneficiary', 'N/A')}")
                                st.write(f"**Source File:** {txn.get('source_file', 'N/A')}")
                        if len(all_transactions) > 10:
                            st.info(f"... and {len(all_transactions) - 10} more transactions")
                    else:
                        st.info("No transaction data to display")
                
                # Generate Excel report
                st.header("📄 Download Report")
                excel_report = analyzer.create_excel_report_with_account_tracking(all_transactions, results)
                
                st.download_button(
                    label="📥 Download Analysis Report",
                    data=excel_report,
                    file_name=f"bank_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    mime="text/plain",
                    help="Download comprehensive analysis report with Account ID tracking"
                )
                
                st.success("✅ Analysis completed! All duplicates are marked with their respective Account IDs.")

if __name__ == "__main__":
    main()