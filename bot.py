import os
import logging
from datetime import datetime, timedelta
import aiohttp
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)
from dotenv import load_dotenv
import base58
import asyncio
import signal
from functools import lru_cache
from typing import List, Dict, Optional, Any, Tuple
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot token
BOT_TOKEN = "8034193873:AAFH0RiaAcRyjkwrXCH8nC9yaxQ-bwFgZyw"

# Conversation states
ANALYZE_WALLET = 0
ADD_TO_FAVORITES = 1
REMOVE_FROM_FAVORITES = 2

class FavoritesManager:
    def __init__(self, filename: str = "favorites.json"):
        self.filename = filename
        self.favorites = self._load_favorites()
        
    def _load_favorites(self) -> Dict[str, Dict[str, str]]:
        """Load favorites from file"""
        try:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.error(f"Error loading favorites: {e}")
            return {}
            
    def _save_favorites(self):
        """Save favorites to file"""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.favorites, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving favorites: {e}")
    
    def add_favorite(self, user_id: str, address: str, name: str) -> bool:
        """Add wallet to favorites"""
        try:
            if user_id not in self.favorites:
                self.favorites[user_id] = {}
            
            self.favorites[user_id][address] = name
            self._save_favorites()
            return True
        except Exception as e:
            logger.error(f"Error adding favorite: {e}")
            return False
            
    def remove_favorite(self, user_id: str, address: str) -> bool:
        """Remove wallet from favorites"""
        try:
            if user_id in self.favorites and address in self.favorites[user_id]:
                del self.favorites[user_id][address]
                self._save_favorites()
                return True
            return False
        except Exception as e:
            logger.error(f"Error removing favorite: {e}")
            return False
            
    def get_favorites(self, user_id: str) -> Dict[str, str]:
        """Get user's favorite wallets"""
        return self.favorites.get(str(user_id), {})

class RPCError(Exception):
    """Custom exception for RPC-related errors"""
    pass

class SolanaAnalyzer:
    def __init__(self):
        self.rpc_url = os.getenv('SOLANA_RPC_URL')
        self.session: Optional[aiohttp.ClientSession] = None
        self.timeout = aiohttp.ClientTimeout(total=30)
        self._token_cache = {}
        self._cache_timeout = 300  # 5 minutes cache timeout
        self._nft_cache = {}
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close the session properly"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def _make_rpc_request(self, method: str, params: list, retries: int = 3) -> Dict:
        """Execute RPC request with retries and rate limiting"""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=self.timeout)

        for attempt in range(retries):
            try:
                await asyncio.sleep(0.1)
                
                async with self.session.post(
                    self.rpc_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": str(time.time()),
                        "method": method,
                        "params": params
                    },
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 429:
                        wait_time = min(1 * (2 ** attempt), 8)  # Max 8 seconds wait
                        logger.warning(f"Rate limit hit, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                    response_text = await response.text()
                    if response.status == 200:
                        data = json.loads(response_text)
                        if "error" in data:
                            raise RPCError(f"RPC Error: {data['error']}")
                        return data
                    else:
                        raise RPCError(f"HTTP {response.status}: {response_text}")

            except (aiohttp.ClientError, json.JSONDecodeError, RPCError) as e:
                if attempt == retries - 1:
                    raise RPCError(f"Failed after {retries} attempts: {str(e)}")
                await asyncio.sleep(1)

        raise RPCError("Max retries exceeded")

    @staticmethod
    def validate_solana_address(address: str) -> bool:
        """Validate Solana address format"""
        try:
            if not isinstance(address, str) or len(address) < 32 or len(address) > 44:
                return False
            decoded = base58.b58decode(address)
            return len(decoded) == 32
        except:
            return False

    async def get_wallet_balance(self, address: str) -> float:
        """Get wallet SOL balance"""
        try:
            response = await self._make_rpc_request("getBalance", [address])
            return float(response['result']['value']) / 1e9
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            return 0.0

    @lru_cache(maxsize=1000)
    async def get_token_symbol(self, mint_address: str) -> str:
        """Get token symbol with caching"""
        cache_key = f"symbol_{mint_address}"
        cached = self._token_cache.get(cache_key)
        if cached and time.time() - cached['timestamp'] < self._cache_timeout:
            return cached['symbol']

        try:
            metadata_response = await self._make_rpc_request(
                "getAccountInfo",
                [mint_address, {"encoding": "jsonParsed"}]
            )
            symbol = mint_address[:4] + '..' + mint_address[-4:]
            self._token_cache[cache_key] = {
                'symbol': symbol,
                'timestamp': time.time()
            }
            return symbol
        except Exception as e:
            logger.error(f"Error getting token symbol: {str(e)}")
            return mint_address[:4] + '..' + mint_address[-4:]

    async def get_token_accounts(self, address: str) -> List[Dict]:
        """Get token accounts with parallel processing"""
        try:
            response = await self._make_rpc_request(
                "getTokenAccountsByOwner",
                [
                    address,
                    {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                    {"encoding": "jsonParsed"}
                ]
            )

            if "error" in response:
                raise RPCError(f"RPC error: {response['error']}")

            accounts = []
            tasks = []
            
            for account in response.get('result', {}).get('value', []):
                try:
                    parsed = account['account']['data']['parsed']['info']
                    amount = float(parsed['tokenAmount']['uiAmount'] or 0)
                    
                    if amount <= 0:
                        continue
                        
                    accounts.append({
                        'mint': parsed['mint'],
                        'amount': amount,
                        'decimals': parsed['tokenAmount']['decimals']
                    })
                    tasks.append(self.get_token_symbol(parsed['mint']))
                except (KeyError, TypeError):
                    continue

            if tasks:
                symbols = await asyncio.gather(*tasks, return_exceptions=True)
                for acc, symbol in zip(accounts, symbols):
                    acc['symbol'] = symbol if not isinstance(symbol, Exception) else acc['mint'][:4] + '..' + acc['mint'][-4:]

            return sorted(accounts, key=lambda x: x['amount'], reverse=True)

        except Exception as e:
            logger.error(f"Error in get_token_accounts: {str(e)}")
            return []

    async def get_recent_transactions(self, address: str, limit: int = 10) -> List[Dict]:
        """Get recent transactions"""
        try:
            response = await self._make_rpc_request(
                "getSignaturesForAddress",
                [address, {"limit": limit}]
            )
            return response.get('result', [])
        except Exception as e:
            logger.error(f"Error getting transactions: {str(e)}")
            return []

    async def test_connection(self) -> bool:
        """Test RPC connection"""
        try:
            response = await self._make_rpc_request("getHealth", [])
            return response.get('result') == "ok"
        except Exception as e:
            logger.error(f"RPC connection test failed: {str(e)}")
            return False

    async def get_nft_collections(self, address: str) -> List[Dict]:
        """Get NFT collections owned by the wallet"""
        try:
            response = await self._make_rpc_request(
                "getProgramAccounts",
                [
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                    {
                        "encoding": "jsonParsed",
                        "filters": [
                            {"dataSize": 165},
                            {"memcmp": {"offset": 32, "bytes": address}}
                        ]
                    }
                ]
            )
            
            nfts = []
            for account in response.get('result', []):
                try:
                    parsed = account['account']['data']['parsed']['info']
                    if parsed['tokenAmount']['decimals'] == 0 and parsed['tokenAmount']['uiAmount'] == 1:
                        nfts.append({
                            'mint': parsed['mint'],
                            'name': await self._get_nft_name(parsed['mint'])
                        })
                except (KeyError, TypeError):
                    continue
                    
            return nfts
        except Exception as e:
            logger.error(f"Error getting NFTs: {str(e)}")
            return []

    async def _get_nft_name(self, mint_address: str) -> str:
        """Get NFT name with caching"""
        if mint_address in self._nft_cache:
            return self._nft_cache[mint_address]
            
        try:
            metadata_response = await self._make_rpc_request(
                "getAccountInfo",
                [mint_address, {"encoding": "jsonParsed"}]
            )
            name = f"NFT {mint_address[:4]}..{mint_address[-4:]}"
            self._nft_cache[mint_address] = name
            return name
        except Exception:
            return f"NFT {mint_address[:4]}..{mint_address[-4:]}"

    async def get_transaction_stats(self, address: str) -> Dict:
        """Get transaction statistics"""
        try:
            week_ago = int((datetime.now() - timedelta(days=7)).timestamp())
            response = await self._make_rpc_request(
                "getSignaturesForAddress",
                [address, {"limit": 100}]
            )
            
            transactions = response.get('result', [])
            total_count = len(transactions)
            
            # Weekly stats
            weekly_txs = [tx for tx in transactions if tx.get('blockTime', 0) > week_ago]
            weekly_count = len(weekly_txs)
            
            # Transaction types
            send_count = sum(1 for tx in transactions if tx.get('err') is None)
            failed_count = total_count - send_count
            
            return {
                'total': total_count,
                'weekly': weekly_count,
                'successful': send_count,
                'failed': failed_count,
                'success_rate': (send_count / total_count * 100) if total_count > 0 else 0
            }
        except Exception as e:
            logger.error(f"Error getting transaction stats: {str(e)}")
            return {'total': 0, 'weekly': 0, 'successful': 0, 'failed': 0, 'success_rate': 0}

    async def get_wallet_age(self, address: str) -> Optional[datetime]:
        """Get wallet age based on earliest transaction"""
        try:
            response = await self._make_rpc_request(
                "getSignaturesForAddress",
                [address, {"limit": 1000}]
            )
            
            transactions = response.get('result', [])
            if transactions:
                earliest_tx = min(tx.get('blockTime', 0) for tx in transactions)
                return datetime.fromtimestamp(earliest_tx)
            return None
        except Exception as e:
            logger.error(f"Error getting wallet age: {str(e)}")
            return None

class SolanaBot:
    def __init__(self):
        self.analyzer = SolanaAnalyzer()
        self.favorites = FavoritesManager()
        self.loading_emojis = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        self.loading_texts = [
            "Connecting to blockchain...",
            "Getting wallet data...",
            "Analyzing tokens...",
            "Checking transactions...",
            "Analyzing NFTs...",
            "Calculating statistics...",
            "Preparing report..."
        ]
        self.temp_address = {}  # Временное хранилище для процесса добавления в избранное

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler for /start command"""
        keyboard = [
            [InlineKeyboardButton("🔍 Analyze Wallet", callback_data='analyze')],
            [InlineKeyboardButton("⭐ Favorites", callback_data='favorites')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "👋 Hello! I'm a Solana wallet analyzer bot.\n\n"
            "I can help you analyze wallet activity "
            "and save your favorite wallets for quick access.",
            reply_markup=reply_markup
        )

    async def button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Button click handler"""
        query = update.callback_query
        await query.answer()
        
        if query.data == 'analyze':
            await query.message.edit_text(
                "📝 Send a Solana wallet address to analyze:"
            )
            return ANALYZE_WALLET
            
        elif query.data == 'favorites':
            return await self.show_favorites(update, context)
            
        elif query.data.startswith('analyze_favorite_'):
            address = query.data.replace('analyze_favorite_', '')
            return await self._handle_single_address(query.message, address, is_edit=True)
            
        elif query.data.startswith('add_favorite_'):
            address = query.data.replace('add_favorite_', '')
            user_id = str(query.from_user.id)
            self.temp_address[user_id] = address
            await query.message.reply_text(
                "✏️ Please send me a name for this wallet (e.g. 'Main Wallet' or 'Trading Account'):"
            )
            return ADD_TO_FAVORITES
            
        elif query.data.startswith('remove_favorite_'):
            address = query.data.replace('remove_favorite_', '')
            if self.favorites.remove_favorite(str(query.from_user.id), address):
                await query.message.edit_text(
                    "✅ Wallet removed from favorites",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⬅️ Back to Favorites", callback_data='favorites')
                    ]])
                )
            return ConversationHandler.END

    async def show_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user's favorite wallets"""
        query = update.callback_query
        user_id = str(query.from_user.id)
        favorites = self.favorites.get_favorites(user_id)
        
        if not favorites:
            keyboard = [[InlineKeyboardButton("🔍 Analyze New Wallet", callback_data='analyze')]]
            await query.message.edit_text(
                "📝 You don't have any favorite wallets yet.\n"
                "Analyze a wallet and use the Add to Favorites button to save it!",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return ConversationHandler.END
        
        text = "⭐ *Your Favorite Wallets:*\n\n"
        keyboard = []
        
        for address, name in favorites.items():
            text += f"*{name}*\n`{address}`\n\n"
            keyboard.extend([
                [
                    InlineKeyboardButton(f"📊 Analyze {name}", callback_data=f'analyze_favorite_{address}'),
                    InlineKeyboardButton("❌", callback_data=f'remove_favorite_{address}')
                ]
            ])
            
        keyboard.append([InlineKeyboardButton("🔍 Add New Wallet", callback_data='analyze')])
        
        await query.message.edit_text(
            text,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return ConversationHandler.END

    async def add_to_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add wallet to favorites handler"""
        if not update.message:
            return ConversationHandler.END
            
        name = update.message.text.strip()
        user_id = str(update.effective_user.id)
        address = self.temp_address.get(user_id)
        
        if not address:
            await update.message.reply_text(
                "❌ Error: Could not find the wallet address. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 Analyze Wallet", callback_data='analyze')
                ]])
            )
            return ConversationHandler.END
            
        if self.favorites.add_favorite(user_id, address, name):
            del self.temp_address[user_id]
            await update.message.reply_text(
                f"✅ Wallet saved to favorites as *{name}*",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⭐ View Favorites", callback_data='favorites')
                ]])
            )
        else:
            await update.message.reply_text(
                "❌ Error saving to favorites. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 Analyze Wallet", callback_data='analyze')
                ]])
            )
            
        return ConversationHandler.END

    async def _handle_single_address(self, message, address: str, is_edit: bool = False) -> int:
        """Handle single address detailed analysis"""
        if not self.analyzer.validate_solana_address(address):
            text = "❌ Invalid address format. Please check and try again."
            try:
                if is_edit:
                    await message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔍 Try Again", callback_data='analyze')
                        ]])
                    )
                else:
                    await message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔍 Try Again", callback_data='analyze')
                        ]])
                    )
            except Exception as e:
                logger.error(f"Error sending invalid address message: {e}")
            return ConversationHandler.END

        try:
            progress_message = await message.reply_text(
                "🔄 Analyzing wallet... This may take a few seconds."
            ) if not is_edit else await message.edit_text(
                "🔄 Analyzing wallet... This may take a few seconds."
            )
        except Exception as e:
            logger.error(f"Error sending initial progress message: {e}")
            return ConversationHandler.END
        
        stop_loading = asyncio.Event()
        loading_task = None
        
        try:
            loading_task = asyncio.create_task(
                self.update_loading_message(
                    progress_message,
                    "Analyzing wallet...",
                    stop_loading
                )
            )
            
            logger.info(f"Starting data gathering for address: {address}")
            balance, token_accounts, transactions, nfts, tx_stats, wallet_age = await asyncio.gather(
                self.analyzer.get_wallet_balance(address),
                self.analyzer.get_token_accounts(address),
                self.analyzer.get_recent_transactions(address),
                self.analyzer.get_nft_collections(address),
                self.analyzer.get_transaction_stats(address),
                self.analyzer.get_wallet_age(address)
            )
            logger.info("Data gathering completed successfully")
            
            report = await self._generate_report(
                address, 
                balance, 
                token_accounts, 
                transactions,
                wallet_age,
                tx_stats,
                nfts
            )
            logger.info("Report generated successfully")
            
            # Add "Add to Favorites" button if the wallet is not in favorites
            user_id = str(message.chat.id)
            favorites = self.favorites.get_favorites(user_id)
            keyboard = []
            
            if address not in favorites:
                keyboard.append([InlineKeyboardButton("⭐ Add to Favorites", callback_data=f'add_favorite_{address}')])
            
            keyboard.append([InlineKeyboardButton("🔄 New Analysis", callback_data='analyze')])
            
            # Stop the loading animation before updating the message
            stop_loading.set()
            if loading_task:
                try:
                    await loading_task
                except Exception as e:
                    logger.error(f"Error waiting for loading task: {e}")
            
            try:
                await progress_message.edit_text(
                    report,
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                logger.info("Final report message sent successfully")
            except Exception as e:
                logger.error(f"Error sending final report: {e}")
                # Try to send as a new message if editing fails
                try:
                    await message.reply_text(
                        report,
                        parse_mode='Markdown',
                        disable_web_page_preview=True,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    logger.info("Final report sent as new message")
                except Exception as e2:
                    logger.error(f"Error sending final report as new message: {e2}")
            
        except Exception as e:
            logger.error(f"Error analyzing wallet: {str(e)}", exc_info=True)
            stop_loading.set()
            if loading_task:
                try:
                    await loading_task
                except Exception as load_err:
                    logger.error(f"Error stopping loading task: {load_err}")
            
            try:
                await progress_message.edit_text(
                    f"❌ Error analyzing wallet: {str(e)}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔄 Try Again", callback_data='analyze')
                    ]])
                )
            except Exception as msg_err:
                logger.error(f"Error sending error message: {msg_err}")
        
        return ConversationHandler.END

    async def analyze_wallet(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Main wallet analysis handler"""
        text = update.message.text.strip()
        addresses = [addr.strip() for addr in text.split() if addr.strip()]
        logger.info(f"Starting analysis for {len(addresses)} addresses")
        
        if len(addresses) > 1:
            return await self._handle_multiple_addresses(update, addresses)
        
        return await self._handle_single_address(update.message, addresses[0])

    async def _handle_multiple_addresses(self, update: Update, addresses: List[str]) -> int:
        """Handle multiple addresses analysis - just return explorer links"""
        try:
            valid_addresses = [addr for addr in addresses if self.analyzer.validate_solana_address(addr)]
            invalid_addresses = [addr for addr in addresses if addr not in valid_addresses]
            
            if invalid_addresses:
                await update.message.reply_text(
                    "❌ Invalid addresses:\n" + "\n".join(invalid_addresses)
                )
            
            if valid_addresses:
                report = []
                for address in valid_addresses:
                    report.extend([
                        f"*Wallet:* `{address}`\n",
                        f"[Solscan](https://solscan.io/account/{address})",
                        f" | [GMGN](https://gmgn.ai/sol/address/{address})\n\n"
                    ])
                
                await update.message.reply_text(
                    "".join(report),
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔍 New Analysis", callback_data='analyze')
                    ]])
                )
            
            return ConversationHandler.END
            
        except Exception as e:
            logger.error(f"Error processing multiple addresses: {str(e)}")
            await update.message.reply_text(
                "❌ Error processing addresses. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 Try Again", callback_data='analyze')
                ]])
            )
            return ConversationHandler.END

    async def update_loading_message(self, message, base_text: str, stop_event):
        """Animated loading message updater"""
        idx = 0
        current_text_idx = 0
        last_update = 0
        error_count = 0
        MAX_ERRORS = 3
        
        while not stop_event.is_set():
            try:
                current_time = time.time()
                if current_time - last_update < 0.5:
                    await asyncio.sleep(0.1)
                    continue

                current_emoji = self.loading_emojis[idx]
                current_text = self.loading_texts[current_text_idx]
                
                try:
                    await message.edit_text(
                        f"{current_emoji} {base_text}\n\n{current_text}"
                    )
                    error_count = 0  # Reset error count on successful update
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error updating loading message (attempt {error_count}): {e}")
                    if error_count >= MAX_ERRORS:
                        logger.error("Too many errors updating loading message, stopping animation")
                        return
                    await asyncio.sleep(1)
                    continue
                
                idx = (idx + 1) % len(self.loading_emojis)
                if idx == 0:
                    current_text_idx = (current_text_idx + 1) % len(self.loading_texts)
                    
                last_update = current_time
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error in loading animation loop (attempt {error_count}): {e}")
                if error_count >= MAX_ERRORS:
                    logger.error("Too many errors in loading animation, stopping")
                    return
                await asyncio.sleep(0.5)

    async def _generate_report(self, address: str, balance: float, token_accounts: List[Dict], transactions: List[Dict], 
                           wallet_age: Optional[datetime], tx_stats: Dict, nfts: List[Dict]) -> str:
        """Generate formatted analysis report"""
        report = [
            f"📊 *Wallet Analysis*\n└ Address: `{address}`\n",
            f"\n💰 *SOL Balance*\n└ {balance:.4f} SOL\n"
        ]
        
        # Add wallet age if available
        if wallet_age:
            days_old = (datetime.now() - wallet_age).days
            report.append(f"\n📅 *Wallet Age*\n└ {days_old} days ({wallet_age.strftime('%Y-%m-%d')})\n")
        
        # Add transaction statistics
        report.extend([
            f"\n📈 *Transaction Statistics*\n",
            f"├ Total Transactions: {tx_stats['total']}\n",
            f"├ Weekly Activity: {tx_stats['weekly']} txs\n",
            f"├ Successful: {tx_stats['successful']}\n",
            f"├ Failed: {tx_stats['failed']}\n",
            f"└ Success Rate: {tx_stats['success_rate']:.1f}%\n"
        ])
        
        if token_accounts:
            report.append(f"\n🪙 *Tokens* (total: {len(token_accounts)})\n")
            for token in token_accounts[:10]:
                report.extend([
                    f"├ {token['symbol']}\n",
                    f"└ Amount: {token['amount']:,.2f}\n"
                ])
            if len(token_accounts) > 10:
                report.append(f"└ _...and {len(token_accounts) - 10} more tokens_\n")
        else:
            report.append("\n🪙 *Tokens*\n└ No active tokens\n")
        
        # Add NFT collections
        if nfts:
            report.append(f"\n🎨 *NFT Collections* (total: {len(nfts)})\n")
            for nft in nfts[:5]:
                report.append(f"└ {nft['name']}\n")
            if len(nfts) > 5:
                report.append(f"└ _...and {len(nfts) - 5} more collections_\n")
        
        if transactions:
            report.append(f"\n📈 *Recent Transactions*\n")
            for tx in transactions[:5]:
                timestamp = datetime.fromtimestamp(tx.get('blockTime', 0))
                status = "✅" if tx.get('err') is None else "❌"
                report.extend([
                    f"├ {status} {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n",
                    f"└ Hash: `{tx['signature'][:8]}...{tx['signature'][-8:]}`\n"
                ])
        
        report.extend([
            f"\n🔍 *Explorers*\n",
            f"├ [Solscan](https://solscan.io/account/{address})\n",
            f"├ [SolanaFM](https://solana.fm/address/{address})\n",
            f"├ [Explorer](https://explorer.solana.com/address/{address})\n",
            f"└ [GMGN](https://gmgn.ai/sol/address/{address})"
        ])
        
        return "".join(report)

async def setup_bot():
    """Setup and configure the bot"""
    application = Application.builder().token(BOT_TOKEN).build()
    bot = SolanaBot()
    
    # Test RPC connection
    if not await bot.analyzer.test_connection():
        logger.error("Failed to connect to Solana RPC")
        return None, None
    
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', bot.start),
            CallbackQueryHandler(bot.button, pattern='^(analyze|favorites|analyze_favorite_|remove_favorite_|add_favorite_)')
        ],
        states={
            ANALYZE_WALLET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.analyze_wallet)
            ],
            ADD_TO_FAVORITES: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.add_to_favorites)
            ]
        },
        fallbacks=[CommandHandler('start', bot.start)]
    )
    
    application.add_handler(conv_handler)
    return application, bot

def main():
    """Main entry point"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        application, bot = loop.run_until_complete(setup_bot())
        if not application:
            return
        
        logger.info("Starting bot...")
        application.run_polling()
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        if 'bot' in locals():
            loop.run_until_complete(bot.analyzer.close())
        logger.info("Cleanup completed")

if __name__ == '__main__':
    main() 