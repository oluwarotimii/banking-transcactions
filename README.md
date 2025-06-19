# Bank Transaction Analyzer

A comprehensive financial forensics tool that analyzes PDF bank statements to detect refunds, duplicate payments, and financial losses with mandatory Account ID tracking.

## Architecture & Logic

### Two-Phase Processing Design

The system follows a **separation of concerns** approach with two distinct phases:

#### Phase 1: Complete Data Extraction
- Extract ALL transactions from ALL PDF documents first
- Process every single page of each PDF file
- Use memory-efficient batching during extraction (1000 transactions per batch)
- Track source file for every transaction
- Generate unique Account IDs from filenames

#### Phase 2: Comprehensive Analysis
- Analyze the complete dataset for patterns
- Detect refunds within same account boundaries
- Identify duplicate transactions across all files
- Calculate financial impact and losses

### Key Features

#### Account ID Tracking
- **Filename-based Account IDs**: Each PDF filename becomes the Account ID
  - `Account_Statement (30).pdf` → Account ID: `Account_Statement (30)`
  - `GTBank_Jan.pdf` → Account ID: `GTBank_Jan`
- **Mandatory Account ID marking**: Every duplicate transaction shows its source Account ID
- **Cross-account duplicate detection**: Identifies duplicates both within same account and across different accounts

#### Memory Management
- Processes large documents (30k+ transactions) efficiently
- Dynamic batching with configurable batch sizes (500-5000 transactions)
- Garbage collection every 50 pages and after each batch
- Separate extraction and analysis phases prevent memory overflow

#### Transaction Analysis
- **Refund Detection**: Matches debits with corresponding credits within same account
- **Duplicate Detection**: Uses fuzzy matching for beneficiary names and amount similarity
- **Financial Impact**: Calculates total losses, refunds, and unmatched debits

## User Instructions Implementation

Based on your requirements:

1. **Account ID Visibility**: Every transaction row shows which file it came from
2. **Complete Extraction**: All pages processed, not just sample data
3. **Memory Efficiency**: Two-phase approach with batching prevents crashes
4. **Duplicate Tracking**: Each duplicate has unique ID including both Account IDs

## Installation & Setup

### Local Installation

1. **Install Python Requirements**:
```bash
pip install -r setup_requirements.txt
```

2. **Run the Application**:
```bash
streamlit run working_app.py --server.port 8501
```

3. **Access the Application**:
- Open browser to `http://localhost:8501`

### Required Dependencies
- streamlit>=1.28.0
- pdfplumber>=0.9.0 (for PDF table extraction)
- fuzzywuzzy>=0.18.0 (for duplicate matching)
- python-levenshtein>=0.12.0 (for string similarity)
- openpyxl>=3.1.0 (for Excel export)
- xlsxwriter>=3.1.0 (for Excel formatting)
- pandas>=2.0.0 (for data manipulation)

## Usage Flow

### Step 1: Upload Bank Statements
- Upload multiple PDF files from same or different banks
- System automatically assigns Account IDs from filenames
- View file breakdown with detected Account IDs

### Step 2: Configure Analysis
- Set duplicate detection window (1-14 days)
- Adjust amount difference threshold (₦1-₦10,000)
- Configure name matching sensitivity (50-100%)
- Select memory management level (500-5000 transactions per batch)

### Step 3: Two-Phase Analysis

#### Phase 1: Extraction
- System processes each PDF completely
- Shows progress: "Page X/Y - Z transactions found"
- Displays extraction summary after each file
- Memory cleanup between files

#### Phase 2: Analysis
- Analyzes complete dataset for patterns
- Generates comprehensive results
- Creates detailed reports

### Step 4: Review Results

#### Summary Tab
- Total transactions, debits, credits
- Estimated financial losses
- Per-account breakdown

#### Refunds Tab
- Transactions that were later credited back
- Shows Account ID for each refund
- Days to refund calculation

#### Duplicates Tab
- Suspected duplicate payments grouped together
- **Each duplicate shows source Account ID**
- Similarity scores and amount differences
- Cross-account duplicate detection

#### All Transactions Tab
- Complete transaction listing with source files
- Breakdown by transaction type
- File-by-file analysis

## Output & Reporting

### Excel Report
- Multi-sheet workbook with color coding
- All transactions with Account ID columns
- Refunded transactions (green highlighting)
- Duplicate groups with Account ID tracking
- Financial summary with loss calculations

### Account ID Integration
Every duplicate transaction includes:
- Original Account ID from filename
- Unique Duplicate ID: `DUP_1_AccountA_AccountB_TXN1`
- Source file tracking
- Cross-reference capabilities

## Real-World Impact

### Financial Recovery
- Identify unrefunded failed transactions
- Detect accidental duplicate payments
- Quantify exact financial losses
- Generate evidence for bank disputes

### Business Intelligence
- Track payment failure patterns
- Monitor refund efficiency
- Audit transaction processes
- Identify systematic issues

This system transforms complex financial forensics into an automated process that can recover significant amounts while providing complete transparency and Account ID traceability across multiple bank statements.