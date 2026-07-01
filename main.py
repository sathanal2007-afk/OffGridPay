from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Optional
import time
import uuid

app = FastAPI(title="OffGridPay - Resilient Offline Ledger Pipeline")

# --- DATA MODELS ---

class Transaction(BaseModel):
    tx_id: str
    from_user: str
    to_user: str
    amount: float
    timestamp: float
    status: str  # "OFFLINE_PENDING" or "SYNCED_WITH_BANK"

class Wallet(BaseModel):
    user_id: str
    allocated_budget: float  # லோக்கல்ல அனுமதிக்கப்பட்ட பட்ஜெட் (On-device Cache)
    current_balance: float   # தற்போதைய பேலன்ஸ்

# --- IN-MEMORY SIMULATION (State Management) ---
wallets_db: Dict[str, Wallet] = {
    "Sathana": Wallet(user_id="Sathana", allocated_budget=5000.0, current_balance=5000.0),
    "Machan": Wallet(user_id="Machan", allocated_budget=2000.0, current_balance=2000.0)
}

pending_sync_queue: List[Transaction] = []
transaction_history: List[Transaction] = []

# சிஸ்டம் ஸ்டேட்ஸ் (இன்டர்வியூல டெமோ காட்ட இத மாத்திக்கலாம்)
SYSTEM_STATUS = {
    "bank_server_online": False,  # பேங்க் சர்வர் டவுன் என்று அஸ்யூம் செய்வோம்
    "network_online": False       # நெட்வொர்க்கும் டவுன்
}

# --- BACKGROUND RECONCILIATION PIPELINE ---
def auto_sync_pipeline():
    """
    நெட்வொர்க் மற்றும் பேங்க் சர்வர் ஆன் ஆகும்போது 
    பேக்ரவுண்டில் லோக்கல் கணக்குகளை பேங்க் சர்வருக்கு சிங்க் செய்யும் பைப்லைன்.
    """
    global pending_sync_queue, transaction_history
    
    if not SYSTEM_STATUS["network_online"] or not SYSTEM_STATUS["bank_server_online"]:
        print("[BG-PIPELINE] Core Network or Bank Server still offline. Holding queue...")
        return

    print(f"[BG-PIPELINE] Connection restored! Syncing {len(pending_sync_queue)} pending transactions to Core Banking System...")
    
    while pending_sync_queue:
        tx = pending_sync_queue.pop(0)
        tx.status = "SYNCED_WITH_BANK"
        transaction_history.append(tx)
        print(f"[BANK-SYNC-SUCCESS] Tx {tx.tx_id}: ₹{tx.amount} successfully reconciled between {tx.from_user} and {tx.to_user}.")

# --- API ENDPOINTS ---

@app.get("/system/status")
def get_system_status():
    return SYSTEM_STATUS

@app.post("/system/toggle")
def toggle_system_status(network: bool, bank: bool, background_tasks: BackgroundTasks):
    """
    நெட்வொர்க் மற்றும் பேங்க் ஸ்டேட்டை மாற்றுவதற்கான எண்ட்பாயிண்ட்.
    """
    SYSTEM_STATUS["network_online"] = network
    SYSTEM_STATUS["bank_server_online"] = bank
    
    # நெட்வொர்க் ஆன் ஆனா உடனே பேக்ரவுண்ட் சிங்க் பைப்லைனை ட்ரிகர் பண்ணு
    if network and bank:
        background_tasks.add_task(auto_sync_pipeline)
        
    return {"message": "System status updated", "current_state": SYSTEM_STATUS}

@app.post("/pay/offline")
def offline_payment(from_user: str, to_user: str, amount: float):
    """
    முக்கியமான கோர் ஃபீச்சர்: பேங்க் சர்வர் டவுனாக இருந்தாலும், 
    நெட்வொர்க் இல்லாவிட்டாலும் லோக்கல் பட்ஜெட்டை வச்சு காசு மாறும்.
    """
    if from_user not in wallets_db or to_user not in wallets_db:
        raise HTTPException(status_code=404, detail="User wallet not found.")
        
    sender_wallet = wallets_db[from_user]
    receiver_wallet = wallets_db[to_user]
    
    # 1. பட்ஜெட் மற்றும் பேலன்ஸ் செக் (Wallet Boundary Guard)
    if sender_wallet.current_balance < amount:
        raise HTTPException(status_code=400, detail="Transaction rejected: Insufficient local offline budget.")
        
    # 2. லோக்கல் லெட்ஜர் மியூட்டேஷன் (Local Ledger Mutation - Offline State Change)
    sender_wallet.current_balance -= amount
    receiver_wallet.current_balance += amount
    
    # 3. கிரியேட் ஆஃப்லைன் கிரிப்டோகிராஃபிக் டிரான்சாக்ஷன்
    tx_id = str(uuid.uuid4())[:8]
    tx = Transaction(
        tx_id=tx_id,
        from_user=from_user,
        to_user=to_user,
        amount=amount,
        timestamp=time.time(),
        status="OFFLINE_PENDING"
    )
    
    # கியூவில் புஷ் பண்ணு
    pending_sync_queue.append(tx)
    
    return {
        "status": "OFFLINE_SUCCESS_SECURED",
        "message": "Payment verified locally via Device Vault. Balance will sync once network is restored.",
        "transaction_details": tx,
        "your_remaining_offline_budget": sender_wallet.current_balance
    }

from fastapi.responses import HTMLResponse

# பிரண்ட்-எண்ட் UI-ஐ லோட் செய்ய ஒரு புது எண்ட்பாயிண்ட்
@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()