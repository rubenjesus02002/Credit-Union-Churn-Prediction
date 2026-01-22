"""
Created using Claude AI. Credit Union Member Churn - Synthetic Data Generator
Generates realistic banking data for 10,000 members over 24 months
Output: SQLite database + CSV exports
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from dataclasses import dataclass
from typing import List, Dict
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
NUM_MEMBERS = 10000
MONTHS_HISTORY = 24
START_DATE = datetime(2022, 1, 1)
END_DATE = START_DATE + timedelta(days=MONTHS_HISTORY * 30)

@dataclass
class PersonaConfig:
    name: str
    proportion: float
    avg_transactions_per_month: int
    balance_range: tuple
    churn_probability: float
    product_adoption_rate: float
    transaction_variance: float

# Define 7 Personas
PERSONAS = [
    PersonaConfig("Primary Banker", 0.20, 45, (5000, 50000), 0.05, 0.85, 0.3),
    PersonaConfig("Rate Shopper", 0.15, 8, (20000, 100000), 0.35, 0.30, 0.2),
    PersonaConfig("Loan-Only", 0.15, 5, (500, 5000), 0.60, 0.15, 0.5),
    PersonaConfig("Slow Adopter", 0.12, 15, (2000, 15000), 0.25, 0.50, 0.6),
    PersonaConfig("Emergency User", 0.10, 25, (100, 3000), 0.40, 0.40, 0.8),
    PersonaConfig("Digital-First", 0.18, 35, (3000, 25000), 0.20, 0.70, 0.4),
    PersonaConfig("Seasonal Worker", 0.10, 20, (500, 8000), 0.45, 0.35, 0.9),
]

class MemberGenerator:
    def __init__(self):
        self.member_id = 1
        self.transaction_id = 1
        self.account_id = 1
        self.loan_id = 1
        self.event_id = 1
        
    def generate_members(self) -> pd.DataFrame:
        """Generate member demographic data"""
        members = []
        
        for persona in PERSONAS:
            num_in_persona = int(NUM_MEMBERS * persona.proportion)
            
            for _ in range(num_in_persona):
                # Random join date within first 18 months
                days_offset = random.randint(0, 540)
                join_date = START_DATE + timedelta(days=days_offset)
                
                # Determine churn
                churned = random.random() < persona.churn_probability
                if churned:
                    # Churn happens 3-18 months after join
                    churn_offset = random.randint(90, 540)
                    churn_date = join_date + timedelta(days=churn_offset)
                    if churn_date > END_DATE:
                        churn_date = None
                        churned = False
                else:
                    churn_date = None
                
                member = {
                    'member_id': self.member_id,
                    'persona': persona.name,
                    'join_date': join_date.strftime('%Y-%m-%d'),
                    'age': random.randint(22, 75),
                    'credit_score': random.randint(580, 850),
                    'income': random.randint(25000, 150000),
                    'zip_code': f"{random.randint(10000, 99999)}",
                    'channel': random.choice(['Branch', 'Online', 'Mobile', 'Referral']),
                    'churned': churned,
                    'churn_date': churn_date.strftime('%Y-%m-%d') if churn_date else None,
                }
                
                members.append(member)
                self.member_id += 1
        
        return pd.DataFrame(members)
    
    def generate_accounts(self, members_df: pd.DataFrame) -> pd.DataFrame:
        """Generate checking, savings, and CD accounts"""
        accounts = []
        
        for _, member in members_df.iterrows():
            persona = next(p for p in PERSONAS if p.name == member['persona'])
            join_date = datetime.strptime(member['join_date'], '%Y-%m-%d')
            
            # Everyone gets checking
            accounts.append({
                'account_id': self.account_id,
                'member_id': member['member_id'],
                'account_type': 'Checking',
                'open_date': member['join_date'],
                'balance': random.uniform(*persona.balance_range) * 0.3,
                'status': 'Closed' if member['churned'] else 'Active',
            })
            self.account_id += 1
            
            # Savings based on adoption rate
            if random.random() < persona.product_adoption_rate:
                open_offset = random.randint(0, 180)
                accounts.append({
                    'account_id': self.account_id,
                    'member_id': member['member_id'],
                    'account_type': 'Savings',
                    'open_date': (join_date + timedelta(days=open_offset)).strftime('%Y-%m-%d'),
                    'balance': random.uniform(*persona.balance_range) * 0.5,
                    'status': 'Closed' if member['churned'] else 'Active',
                })
                self.account_id += 1
            
            # CD for rate shoppers and primary bankers
            if member['persona'] in ['Rate Shopper', 'Primary Banker'] and random.random() < 0.7:
                open_offset = random.randint(30, 365)
                accounts.append({
                    'account_id': self.account_id,
                    'member_id': member['member_id'],
                    'account_type': 'CD',
                    'open_date': (join_date + timedelta(days=open_offset)).strftime('%Y-%m-%d'),
                    'balance': random.uniform(*persona.balance_range),
                    'status': 'Closed' if member['churned'] else 'Active',
                })
                self.account_id += 1
        
        return pd.DataFrame(accounts)
    
    def generate_transactions(self, members_df: pd.DataFrame, accounts_df: pd.DataFrame) -> pd.DataFrame:
        """Generate transaction history - this is the big one!"""
        print("Generating transactions (this will take a moment for 7M+ rows)...")
        transactions = []
        
        for _, member in members_df.iterrows():
            if len(transactions) % 1000 == 0:
                print(f"  Processed {len(transactions):,} transactions for {_} members...")
            
            persona = next(p for p in PERSONAS if p.name == member['persona'])
            join_date = datetime.strptime(member['join_date'], '%Y-%m-%d')
            
            # Get member's checking account
            checking = accounts_df[(accounts_df['member_id'] == member['member_id']) & 
                                  (accounts_df['account_type'] == 'Checking')].iloc[0]
            
            # Determine active period
            if member['churned'] and member['churn_date']:
                end_active = datetime.strptime(member['churn_date'], '%Y-%m-%d')
            else:
                end_active = END_DATE
            
            # Generate transactions month by month
            current_date = join_date
            while current_date < end_active:
                # Number of transactions this month (with variance)
                base_trans = persona.avg_transactions_per_month
                variance = int(base_trans * persona.transaction_variance)
                num_trans = max(1, random.randint(base_trans - variance, base_trans + variance))
                
                for _ in range(num_trans):
                    trans_date = current_date + timedelta(days=random.randint(0, 28))
                    
                    if trans_date > end_active:
                        break
                    
                    # Transaction types based on persona
                    if member['persona'] == 'Primary Banker':
                        trans_type = random.choices(
                            ['Direct Deposit', 'Debit Card', 'ACH Payment', 'Check', 'ATM Withdrawal', 'Transfer'],
                            weights=[0.15, 0.35, 0.25, 0.10, 0.10, 0.05]
                        )[0]
                    elif member['persona'] == 'Digital-First':
                        trans_type = random.choices(
                            ['Direct Deposit', 'Debit Card', 'Mobile Payment', 'P2P Transfer', 'ATM Withdrawal'],
                            weights=[0.20, 0.40, 0.20, 0.15, 0.05]
                        )[0]
                    else:
                        trans_type = random.choices(
                            ['Direct Deposit', 'Debit Card', 'ACH Payment', 'ATM Withdrawal', 'Check'],
                            weights=[0.15, 0.40, 0.20, 0.15, 0.10]
                        )[0]
                    
                    # Amount based on transaction type
                    if trans_type == 'Direct Deposit':
                        amount = random.uniform(800, member['income'] / 24)
                    elif 'Debit' in trans_type or 'Payment' in trans_type:
                        amount = -random.uniform(5, 500)
                    elif 'Withdrawal' in trans_type:
                        amount = -random.choice([20, 40, 60, 80, 100, 200])
                    else:
                        amount = random.uniform(-200, 200)
                    
                    transactions.append({
                        'transaction_id': self.transaction_id,
                        'account_id': checking['account_id'],
                        'member_id': member['member_id'],
                        'transaction_date': trans_date.strftime('%Y-%m-%d'),
                        'transaction_type': trans_type,
                        'amount': round(amount, 2),
                        'merchant_category': random.choice(['Retail', 'Grocery', 'Gas', 'Restaurant', 
                                                           'Utilities', 'Entertainment', 'Other']) if amount < 0 else 'Income',
                    })
                    self.transaction_id += 1
                
                current_date += timedelta(days=30)
        
        print(f"✓ Generated {len(transactions):,} transactions")
        return pd.DataFrame(transactions)
    
    def generate_loans(self, members_df: pd.DataFrame) -> pd.DataFrame:
        """Generate loan data"""
        loans = []
        
        for _, member in members_df.iterrows():
            join_date = datetime.strptime(member['join_date'], '%Y-%m-%d')
            
            # Loan-Only persona always gets auto loan
            if member['persona'] == 'Loan-Only':
                loan_date = join_date + timedelta(days=random.randint(0, 30))
                loans.append({
                    'loan_id': self.loan_id,
                    'member_id': member['member_id'],
                    'loan_type': 'Auto',
                    'origination_date': loan_date.strftime('%Y-%m-%d'),
                    'original_amount': random.randint(15000, 35000),
                    'current_balance': random.randint(0, 10000),
                    'interest_rate': round(random.uniform(3.5, 7.5), 2),
                    'term_months': random.choice([36, 48, 60, 72]),
                    'status': 'Paid Off' if member['churned'] else 'Active',
                })
                self.loan_id += 1
            
            # Others might have loans based on persona
            elif random.random() < 0.3:
                loan_date = join_date + timedelta(days=random.randint(90, 540))
                loan_type = random.choices(
                    ['Auto', 'Personal', 'HELOC', 'Mortgage'],
                    weights=[0.5, 0.3, 0.1, 0.1]
                )[0]
                
                amount_ranges = {
                    'Auto': (15000, 35000),
                    'Personal': (5000, 25000),
                    'HELOC': (20000, 100000),
                    'Mortgage': (150000, 500000),
                }
                
                loans.append({
                    'loan_id': self.loan_id,
                    'member_id': member['member_id'],
                    'loan_type': loan_type,
                    'origination_date': loan_date.strftime('%Y-%m-%d'),
                    'original_amount': random.randint(*amount_ranges[loan_type]),
                    'current_balance': random.randint(0, amount_ranges[loan_type][1]),
                    'interest_rate': round(random.uniform(3.5, 12.0), 2),
                    'term_months': random.choice([36, 60, 84, 120, 240, 360]),
                    'status': random.choice(['Active', 'Paid Off']) if not member['churned'] else 'Closed',
                })
                self.loan_id += 1
        
        return pd.DataFrame(loans)
    
    def generate_events(self, members_df: pd.DataFrame) -> pd.DataFrame:
        """Generate customer service events and churn signals"""
        events = []
        
        for _, member in members_df.iterrows():
            join_date = datetime.strptime(member['join_date'], '%Y-%m-%d')
            persona = next(p for p in PERSONAS if p.name == member['persona'])
            
            # Emergency User gets PALs events
            if member['persona'] == 'Emergency User':
                num_pals = random.randint(2, 8)
                for _ in range(num_pals):
                    event_date = join_date + timedelta(days=random.randint(30, 600))
                    events.append({
                        'event_id': self.event_id,
                        'member_id': member['member_id'],
                        'event_date': event_date.strftime('%Y-%m-%d'),
                        'event_type': 'PAL_Request',
                        'event_detail': f"Amount: ${random.choice([200, 500, 1000])}",
                    })
                    self.event_id += 1
            
            # Customer service contacts
            num_contacts = random.randint(0, 5)
            for _ in range(num_contacts):
                event_date = join_date + timedelta(days=random.randint(0, 600))
                events.append({
                    'event_id': self.event_id,
                    'member_id': member['member_id'],
                    'event_date': event_date.strftime('%Y-%m-%d'),
                    'event_type': random.choice(['Call_Center', 'Branch_Visit', 'Email', 'Chat']),
                    'event_detail': random.choice(['Account Question', 'Fraud Alert', 'Rate Inquiry', 
                                                  'Technical Issue', 'Product Info']),
                })
                self.event_id += 1
            
            # Churn signals before actual churn
            if member['churned'] and member['churn_date']:
                churn_date = datetime.strptime(member['churn_date'], '%Y-%m-%d')
                
                # Declining balance signal
                signal_date = churn_date - timedelta(days=60)
                events.append({
                    'event_id': self.event_id,
                    'member_id': member['member_id'],
                    'event_date': signal_date.strftime('%Y-%m-%d'),
                    'event_type': 'Balance_Decline',
                    'event_detail': 'Balance dropped >50% in 30 days',
                })
                self.event_id += 1
                
                # Inactivity signal
                signal_date = churn_date - timedelta(days=30)
                events.append({
                    'event_id': self.event_id,
                    'member_id': member['member_id'],
                    'event_date': signal_date.strftime('%Y-%m-%d'),
                    'event_type': 'Inactivity',
                    'event_detail': 'No transactions in 30 days',
                })
                self.event_id += 1
        
        return pd.DataFrame(events)

def create_database(output_path='credit_union_data.db'):
    """Main function to generate all data and create SQLite database"""
    print("=" * 60)
    print("CREDIT UNION SYNTHETIC DATA GENERATOR")
    print(f"Generating data for {NUM_MEMBERS:,} members over {MONTHS_HISTORY} months")
    print("=" * 60)
    
    generator = MemberGenerator()
    
    # Generate all tables
    print("\n1. Generating members...")
    members_df = generator.generate_members()
    print(f"   ✓ Created {len(members_df):,} members")
    
    print("\n2. Generating accounts...")
    accounts_df = generator.generate_accounts(members_df)
    print(f"   ✓ Created {len(accounts_df):,} accounts")
    
    print("\n3. Generating transactions...")
    transactions_df = generator.generate_transactions(members_df, accounts_df)
    
    print("\n4. Generating loans...")
    loans_df = generator.generate_loans(members_df)
    print(f"   ✓ Created {len(loans_df):,} loans")
    
    print("\n5. Generating events...")
    events_df = generator.generate_events(members_df)
    print(f"   ✓ Created {len(events_df):,} events")
    
    # Create SQLite database
    print(f"\n6. Creating SQLite database: {output_path}")
    if os.path.exists(output_path):
        os.remove(output_path)
    
    conn = sqlite3.connect(output_path)
    
    members_df.to_sql('members', conn, index=False)
    accounts_df.to_sql('accounts', conn, index=False)
    transactions_df.to_sql('transactions', conn, index=False)
    loans_df.to_sql('loans', conn, index=False)
    events_df.to_sql('events', conn, index=False)
    
    # Create indexes for better query performance
    cursor = conn.cursor()
    cursor.execute('CREATE INDEX idx_trans_member ON transactions(member_id)')
    cursor.execute('CREATE INDEX idx_trans_date ON transactions(transaction_date)')
    cursor.execute('CREATE INDEX idx_accounts_member ON accounts(member_id)')
    cursor.execute('CREATE INDEX idx_events_member ON events(member_id)')
    
    conn.commit()
    
    # Export summary CSVs
    print("\n7. Exporting summary CSVs...")
    os.makedirs('csv_exports', exist_ok=True)
    members_df.to_csv('csv_exports/members.csv', index=False)
    accounts_df.to_csv('csv_exports/accounts.csv', index=False)
    
    # Sample of transactions (first 100k for quick viewing)
    transactions_df.head(100000).to_csv('csv_exports/transactions_sample.csv', index=False)
    
    loans_df.to_csv('csv_exports/loans.csv', index=False)
    events_df.to_csv('csv_exports/events.csv', index=False)
    
    print("   ✓ Exported to csv_exports/ folder")
    
    # Generate summary statistics
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE - SUMMARY STATISTICS")
    print("=" * 60)
    print(f"Total Members:       {len(members_df):>12,}")
    print(f"Total Accounts:      {len(accounts_df):>12,}")
    print(f"Total Transactions:  {len(transactions_df):>12,}")
    print(f"Total Loans:         {len(loans_df):>12,}")
    print(f"Total Events:        {len(events_df):>12,}")
    print(f"\nChurn Rate:          {members_df['churned'].mean()*100:>11.1f}%")
    print(f"\nDatabase Size:       ~{os.path.getsize(output_path) / 1024 / 1024:.1f} MB")
    print("=" * 60)
    
    print("\nPersona Distribution:")
    print(members_df['persona'].value_counts().sort_index())
    
    print("\n✓ Database created successfully!")
    print(f"\nNext steps:")
    print(f"1. Open 'credit_union_data.db' with DB Browser for SQLite")
    print(f"2. Review CSV summaries in 'csv_exports/' folder")
    print(f"3. Start building your churn prediction model!")
    
    conn.close()
    return members_df, accounts_df, transactions_df, loans_df, events_df

if __name__ == "__main__":
    # Run the generator
    create_database()