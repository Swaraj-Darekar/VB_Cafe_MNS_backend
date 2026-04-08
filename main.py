from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
from supabase import create_client, Client
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
import pytz
import asyncio
import httpx
import ujson
from contextlib import asynccontextmanager

# Load environment variables from .env
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Lifespan & Keep-Alive ---
async def ping_server():
    """Background task to ping the server itself to keep it awake on Render."""
    await asyncio.sleep(60) # Wait 1 min after startup
    url = os.environ.get("RENDER_EXTERNAL_URL") or "http://127.0.0.1:8000"
    print(f"Self-ping task started (targeting {url}/health)")
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Ping the health endpoint
                resp = await client.get(f"{url}/health")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Heartbeat: {resp.status_code}")
            except Exception as e:
                print(f"Self-ping failed: {e}")
            
            # Wait for 10 minutes (600 seconds)
            await asyncio.sleep(600)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the keep-alive background task
    asyncio.create_task(ping_server())
    yield
    # Shutdown logic (if any) could go here

# Initialize FastAPI App with custom lifespan and faster JSON responses
from fastapi.responses import UJSONResponse

app = FastAPI(
    title="VB Cafe Management System API",
    lifespan=lifespan,
    default_response_class=UJSONResponse
)

# Configure CORS so the React Frontend can communicate with the Python backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Global Exception Handler for Debugging ---
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    import traceback
    err_msg = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    logger.error(f"Unhandled Exception: {err_msg}")
    return UJSONResponse(
        status_code=500,
        content={"status": "error", "message": str(exc), "traceback": err_msg}
    )

# Initialize Supabase Client (singleton — created once at startup)
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Warning: Supabase credentials not found in backend/.env")
    supabase = None
else:
    supabase: Client = create_client(url, key)

# --- Pydantic Models ---
class CategoryCreate(BaseModel):
    name: str

class MenuItemCreate(BaseModel):
    category_id: str
    name: str
    price: float
    in_stock: bool = True

class MenuItemUpdate(BaseModel):
    price: float

class OrderItemCreate(BaseModel):
    item_id: str
    name: str
    price: float
    qty: int

class OrderCreate(BaseModel):
    items: List[OrderItemCreate]
    total_amount: float
    payment_mode: str
    discount: float

class WalletTopup(BaseModel):
    amount: float

class SettingsUpdate(BaseModel):
    commission_rs: float

class ExpenseCreate(BaseModel):
    amount: float
    description: str
    created_at: Optional[str] = None

class LoginRequest(BaseModel):
    username: str
    password: str
    is_super: bool

# --- General Routes ---
@app.get("/")
def read_root():
    return {"message": "Welcome to the VB Cafe MNS Backend Server"}

@app.get("/health")
def health_check():
    if supabase:
        return {"status": "healthy", "database": "connected"}
    return {"status": "degraded", "database": "disconnected"}

@app.post("/api/login")
async def login(req: LoginRequest):
    # Hardcoded as requested by user, but moved to backend for security
    if req.is_super:
        if req.username == "superadmin" and req.password == "superpassword":
            return {"status": "success", "user": "superadmin"}
    else:
        if req.username == "admin" and req.password == "Ybs123":
            return {"status": "success", "user": "admin"}
            
    raise HTTPException(status_code=401, detail="Invalid credentials")

# --- Category Routes ---
@app.get("/api/categories")
def get_categories():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("menu_categories").select("*").order("name").execute()
    return {"data": response.data}

@app.post("/api/categories")
def create_category(category: CategoryCreate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    # Single insert — let the DB unique constraint reject duplicates instead of doing a separate SELECT
    try:
        response = supabase.table("menu_categories").insert({"name": category.name}).execute()
        return {"message": "Category created successfully", "data": response.data}
    except Exception as e:
        err_str = str(e).lower()
        if "duplicate" in err_str or "unique" in err_str or "23505" in err_str:
            raise HTTPException(status_code=400, detail="Category already exists")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/categories/{category_id}")
def update_category(category_id: str, category: CategoryCreate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        response = supabase.table("menu_categories").update({"name": category.name}).eq("id", category_id).execute()
        return {"message": "Category updated successfully", "data": response.data}
    except Exception as e:
        print(f"CATEGORY_UPDATE_FAILED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/categories/{category_id}")
def delete_category(category_id: str):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        response = supabase.table("menu_categories").delete().eq("id", category_id).execute()
        return {"message": "Category deleted successfully", "data": response.data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot delete category: {str(e)}")

# --- Menu Item Routes ---
@app.get("/api/menu_items")
def get_menu_items():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("menu_items").select("*, menu_categories(name)").order("name").execute()
    return {"data": response.data}

@app.post("/api/menu_items")
def create_menu_item(item: MenuItemCreate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    insert_data = {
        "category_id": item.category_id,
        "name": item.name,
        "price": item.price,
        "in_stock": item.in_stock
    }
    response = supabase.table("menu_items").insert(insert_data).execute()
    return {"message": "Menu item created successfully", "data": response.data}

@app.patch("/api/menu_items/{item_id}")
def update_menu_item(item_id: str, update_data: MenuItemUpdate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("menu_items").update({"price": update_data.price}).eq("id", item_id).execute()
    return {"message": "Menu item updated successfully", "data": response.data}

@app.delete("/api/menu_items/{item_id}")
def delete_menu_item(item_id: str):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("menu_items").delete().eq("id", item_id).execute()
    return {"message": "Menu item deleted successfully", "data": response.data}

# --- Order & Wallet Routes ---
@app.post("/api/orders")
def create_order(order: OrderCreate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")

    # Optimization: Use fixed commission if settings fetch fails, 
    # but try to fetch them in one go if possible.
    commission_rs = 2.0
    current_wallet = {"id": None, "balance": 0.0}

    try:
        # Step 1: Parallel-ish fetch using the fact that these are quick SELECTs
        # In a high-perf environment we'd use asyncio.gather here, 
        # but even sequential is better if we reduce the number of rows.
        
        settings_data = supabase.table("settings").select("commission_rs").limit(1).execute().data
        if settings_data:
            commission_rs = float(settings_data[0]['commission_rs'])

        wallet_data = supabase.table("wallet").select("id, balance").limit(1).execute().data
        if wallet_data:
            current_wallet = {"id": wallet_data[0]['id'], "balance": float(wallet_data[0]['balance'])}
        else:
            # Initialize wallet if it doesn't exist
            init = supabase.table("wallet").insert({"balance": 0.0}).execute().data
            if init:
                current_wallet = {"id": init[0]['id'], "balance": 0.0}

        if current_wallet["balance"] < 10:
             raise HTTPException(status_code=403, detail="Insufficient wallet balance. Minimum ₹10 required.")

        # Step 2: Get token number (one quick count)
        token_no = (supabase.table("orders").select("id", count="exact").execute().count or 0) + 1

        # Step 3: Insert order
        order_resp = supabase.table("orders").insert({
            "total_amount": order.total_amount,
            "payment_mode": order.payment_mode,
            "discount": order.discount,
            "is_settled": False,
            "token_no": token_no
        }).execute().data

        if not order_resp:
            raise HTTPException(status_code=400, detail="Failed to create order")

        order_id = order_resp[0]['id']

        # Step 4: Batch insert items
        items_to_insert = [
            {
                "order_id": order_id,
                "item_id": item.item_id,
                "item_name": item.name,
                "price_at_time": item.price,
                "qty": item.qty
            } for item in order.items
        ]
        supabase.table("order_items").insert(items_to_insert).execute()

        # Step 5: Update wallet balance
        new_balance = max(0, current_wallet["balance"] - commission_rs)
        if current_wallet["id"]:
            supabase.table("wallet").update({"balance": new_balance}).eq("id", current_wallet["id"]).execute()

        return {
            "message": "Order created successfully",
            "order_id": order_id,
            "token_no": token_no,
            "new_balance": new_balance
        }

    except HTTPException: raise
    except Exception as e:
        print(f"ORDER_CREATION_FAILED: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/orders")
def get_orders():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("orders").select("*, order_items(*)").eq("is_settled", False).order("created_at", desc=True).execute()
    return {"data": response.data}

@app.get("/api/wallet/balance")
def get_wallet_balance():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        response = supabase.table("wallet").select("balance").limit(1).execute()
        if response.data:
            return {"balance": response.data[0]['balance']}
        return {"balance": 0.0}
    except Exception:
        return {"balance": 100.0}

@app.post("/api/wallet/add")
def add_wallet_money(topup: WalletTopup):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        curr = supabase.table("wallet").select("id, balance").limit(1).execute()
        if curr.data:
            new_bal = float(curr.data[0]['balance']) + topup.amount
            supabase.table("wallet").update({"balance": new_bal}).eq("id", curr.data[0]['id']).execute()
            return {"message": "Wallet topped up", "new_balance": new_bal}
        else:
            supabase.table("wallet").insert({"balance": topup.amount}).execute()
            return {"message": "Wallet initialized", "new_balance": topup.amount}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/settings")
def get_settings():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        resp = supabase.table("settings").select("*").limit(1).execute()
        if resp.data:
            return resp.data[0]
        return {"commission_rs": 2.0}
    except Exception:
        return {"commission_rs": 2.0}

@app.post("/api/settings")
def update_settings(settings: SettingsUpdate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        curr = supabase.table("settings").select("id").limit(1).execute()
        if curr.data:
            supabase.table("settings").update({"commission_rs": settings.commission_rs}).eq("id", curr.data[0]['id']).execute()
        else:
            supabase.table("settings").insert({"commission_rs": settings.commission_rs}).execute()
        return {"message": "Settings updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Expense Routes ---
@app.get("/api/expenses")
def get_expenses():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("expenses").select("*").eq("is_settled", False).order("created_at", desc=True).execute()
    return {"data": response.data}

@app.post("/api/expenses")
def create_expense(expense: ExpenseCreate):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    data = {
        "amount": expense.amount,
        "description": expense.description,
        "is_settled": False
    }
    if expense.created_at:
        data["created_at"] = expense.created_at
    response = supabase.table("expenses").insert(data).execute()
    return {"message": "Expense recorded successfully", "data": response.data}

@app.delete("/api/expenses/{expense_id}")
def delete_expense(expense_id: str):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    supabase.table("expenses").delete().eq("id", expense_id).execute()
    return {"message": "Expense deleted successfully"}

# --- Analytics Routes ---
@app.get("/api/analytics/summary")
def get_analytics_summary():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")

    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)
    
    # Precise boundaries for DB filtering
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    month_start = today_start.replace(day=1)

    # Use ISO strings for filtering
    today_iso = today_start.isoformat()
    yesterday_iso = yesterday_start.isoformat()
    month_iso = month_start.isoformat()

    # Optimization: Only fetch what's needed for the 3 buckets in fewer queries if possible, 
    # but for now, DB-level filtering on all_orders is better than Python-level.
    # Actually, let's fetch all UNSETTLED orders but filter by date in the query.
    
    # We'll fetch all unsettled orders from the start of the current month.
    # This is much smaller than ALL unsettled orders if they haven't settled for a long time.
    try:
        all_orders = supabase.table("orders") \
            .select("total_amount, payment_mode, created_at") \
            .eq("is_settled", False) \
            .gte("created_at", month_iso) \
            .execute().data or []
            
        all_expenses = supabase.table("expenses") \
            .select("amount, created_at") \
            .eq("is_settled", False) \
            .gte("created_at", month_iso) \
            .execute().data or []
            
        # Also need total unsettled count for "All-Time Bills" which is actually "Unsettled Bills"
        total_count_resp = supabase.table("orders").select("id", count="exact").eq("is_settled", False).execute()
        total_unsettled_count = total_count_resp.count or 0

    except Exception as e:
        print(f"ANALYTICS_QUERY_FAILED: {e}")
        return {"today": {"count": 0, "total":0}, "yesterday": {"count": 0, "total":0}, "monthly": {"sales": 0, "expenses":0, "profit":0}}

    def parse_dt(dt_str):
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00')).astimezone(tz)

    def get_stats(orders_list):
        total = sum(float(o['total_amount']) for o in orders_list)
        online = sum(float(o['total_amount']) for o in orders_list if o['payment_mode'] == 'Online')
        cash = sum(float(o['total_amount']) for o in orders_list if o['payment_mode'] == 'Cash')
        return {"total": total, "online": online, "cash": cash, "count": len(orders_list)}

    today_orders, yesterday_orders = [], []
    monthly_sales = 0.0
    
    for o in all_orders:
        dt = parse_dt(o['created_at'])
        amount = float(o['total_amount'])
        monthly_sales += amount
        if dt >= today_start:
            today_orders.append(o)
        elif dt >= yesterday_start:
            yesterday_orders.append(o)

    monthly_expenses = sum(float(e['amount']) for e in all_expenses)

    return {
        "today": get_stats(today_orders),
        "yesterday": get_stats(yesterday_orders),
        "monthly": {
            "sales": monthly_sales,
            "expenses": monthly_expenses,
            "profit": monthly_sales - monthly_expenses,
            "count": total_unsettled_count # Using this for the "All-Time" display in SuperAdmin
        }
    }

# --- Monthly Settlement Routes ---
@app.get("/api/settlements")
def get_settlements():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("monthly_settlements").select("*").order("settled_at", desc=True).execute()
    return {"data": response.data}

@app.get("/api/settlements/{settlement_id}/expenses")
def get_settlement_expenses(settlement_id: str):
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    response = supabase.table("expenses").select("*").eq("settlement_id", settlement_id).execute()
    return {"data": response.data}

@app.post("/api/analytics/settle")
def settle_monthly_data():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")

    tz = pytz.timezone('Asia/Kolkata')
    now = datetime.now(tz)
    month_label = now.strftime("%B %Y")

    unsettled_orders = supabase.table("orders").select("id, total_amount").eq("is_settled", False).execute().data or []
    unsettled_expenses = supabase.table("expenses").select("id, amount").eq("is_settled", False).execute().data or []

    if not unsettled_orders and not unsettled_expenses:
        return {"message": "No data to settle"}

    total_sales = sum(float(o['total_amount']) for o in unsettled_orders)
    total_expenses = sum(float(e['amount']) for e in unsettled_expenses)
    net_profit = total_sales - total_expenses

    settlement_resp = supabase.table("monthly_settlements").insert({
        "month_label": month_label,
        "total_sales": total_sales,
        "total_expenses": total_expenses,
        "net_profit": net_profit
    }).execute()

    if not settlement_resp.data:
        raise HTTPException(status_code=400, detail="Failed to create settlement record")

    settlement_id = settlement_resp.data[0]['id']

    if unsettled_orders:
        order_ids = [o['id'] for o in unsettled_orders]
        supabase.table("orders").update({"is_settled": True, "settlement_id": settlement_id}).in_("id", order_ids).execute()

    if unsettled_expenses:
        expense_ids = [e['id'] for e in unsettled_expenses]
        supabase.table("expenses").update({"is_settled": True, "settlement_id": settlement_id}).in_("id", expense_ids).execute()

    return {
        "message": f"Successfully settled {month_label}",
        "settlement_id": settlement_id,
        "totals": {"sales": total_sales, "expenses": total_expenses, "profit": net_profit}
    }

# --- System Administration ---
@app.post("/api/system/reset")
def reset_system_data():
    if not supabase:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    try:
        supabase.table("order_items").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("orders").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("expenses").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("monthly_settlements").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("menu_items").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        supabase.table("menu_categories").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

        wallet_data = supabase.table("wallet").select("id").limit(1).execute()
        if wallet_data.data:
            supabase.table("wallet").update({"balance": 0.0}).eq("id", wallet_data.data[0]['id']).execute()
        else:
            supabase.table("wallet").insert({"balance": 0.0}).execute()

        return {"message": "System reset successfully. All transaction and menu data cleared."}
    except Exception as e:
        print(f"RESET_FAILED: {e}")
        raise HTTPException(status_code=500, detail=str(e))
