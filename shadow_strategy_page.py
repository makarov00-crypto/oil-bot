from __future__ import annotations


def build_shadow_strategy_page(site_nav: str) -> str:
    """The distinct conditional exit experiment, without duplicate entry analytics."""
    page = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow"><title>Гипотеза выхода · Oil Bot</title>
<link rel="icon" href="/favicon.ico" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{--bg:#030711;--ink:#edf6ff;--muted:#93a8c3;--line:rgba(102,174,255,.18)}
*{box-sizing:border-box}body{margin:0;min-height:100vh;color:var(--ink);background:linear-gradient(180deg,#08111f,var(--bg) 48%);font-family:Manrope,sans-serif}
.site-header{position:sticky;top:0;z-index:20;background:rgba(4,9,18,.94);backdrop-filter:blur(16px);border-bottom:1px solid var(--line)}
.site-header__inner,.wrap{width:min(1280px,100%);margin:auto}.site-header__inner{padding:18px 28px;display:flex;align-items:center;justify-content:space-between;gap:20px}
.site-brand__eyebrow{margin-bottom:6px;color:#14f1ff;font:700 12px/1 'JetBrains Mono',monospace;letter-spacing:.16em;text-transform:uppercase}.site-brand__title{font:700 18px/1.1 Sora,sans-serif}
.site-nav{display:flex;flex-wrap:wrap;gap:10px}.site-nav__link{padding:10px 14px;color:#b8cae3;text-decoration:none;font-weight:600;border:1px solid var(--line);border-radius:8px}.site-nav__link.is-active{color:white;background:rgba(67,197,255,.16);border-color:rgba(67,197,255,.44)}
.wrap{padding:32px 28px 64px}h1,h2,h3,p{margin-top:0}h1,h2,h3{font-family:Sora,sans-serif}h1{font-size:32px}h2{font-size:22px}h3{font-size:16px}
.head{display:flex;justify-content:space-between;gap:20px;padding-bottom:24px;border-bottom:1px solid var(--line)}.head p{max-width:760px;color:var(--muted);line-height:1.6}.stamp{color:var(--muted);font:12px 'JetBrains Mono',monospace;white-space:nowrap}
.section{padding:26px 0;border-bottom:1px solid var(--line)}.section>p{color:var(--muted);line-height:1.6;max-width:900px}.notice{border:1px solid var(--line);border-radius:10px;background:rgba(8,17,32,.78);padding:16px 18px;line-height:1.6;color:#cfdded}
.foot{margin-top:24px;color:var(--muted);font-size:12px;line-height:1.6}
@media(max-width:850px){.head{display:block}.stamp{margin-top:10px}}
@media(max-width:600px){.site-header__inner{display:block}.site-nav{margin-top:14px}.wrap,.site-header__inner{padding-left:16px;padding-right:16px}}
</style></head><body>__NAV__
<main class="wrap"><header class="head"><div><h1>Исследование выходов</h1><p>Здесь остаётся только отдельная гипотеза условного удержания позиции. Кандидаты, решения ИИ и реальные сделки показаны на основном дашборде.</p></div><div class="stamp" id="stamp">Загрузка…</div></header>
<section class="section"><h2>Гипотеза выхода</h2><p>Единственная оставленная проверка выходов: что было бы при условном удержании ещё двух часовых свечей после сигнала истощения AO. Гипотеза не меняет реальные выходы.</p><div id="exitExperiment" class="notice"></div></section>
<p class="foot">Наблюдения и контрфактические расчёты не являются торговыми сигналами. Решение о настройке правил требует выборки по самой AO/Чайкин стратегии и проверки результата после издержек.</p></main>
<script>
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time=v=>{if(!v)return '—';const d=new Date(v);return isNaN(d)?esc(v):d.toLocaleString('ru-RU',{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})};
function render(data){
 document.getElementById('stamp').textContent='Обновлено '+time(data.generated_at)+' МСК';
 const e=data.exit_experiment||{},ready=e.readiness||{};const count=Number(e.evaluated||0),target=Number(ready.target_evaluated||20);const delta=e.delta_rub_1lot==null?'—':Number(e.delta_rub_1lot).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2})+' ₽/лот';
 document.getElementById('exitExperiment').innerHTML='<strong>'+count+' из '+target+' наблюдений</strong> · '+esc(delta)+' к обычному выходу.<br>'+(ready.status==='ready_for_limited_trial'?'Условия первичной проверки выполнены; нужен отдельный разбор перед изменением правила.':ready.status==='not_confirmed'?'Гипотеза не подтвердилась на текущей выборке.':'Выборка пока мала для вывода.');
}
fetch('/api/strategy-research',{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('HTTP '+r.status);return r.json()}).then(render).catch(e=>{document.getElementById('stamp').textContent='Ошибка загрузки';document.getElementById('exitExperiment').textContent=String(e)});
</script></body></html>"""
    return page.replace("__NAV__", site_nav)
