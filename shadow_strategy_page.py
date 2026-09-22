from __future__ import annotations


def build_shadow_strategy_page(site_nav: str) -> str:
    """Research view replacing the obsolete shadow/live comparison."""
    page = """<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow"><title>Исследования · Oil Bot</title>
<link rel="icon" href="/favicon.ico" type="image/svg+xml">
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Sora:wght@500;600;700&family=Manrope:wght@400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>
:root{--bg:#030711;--surface:#081120;--ink:#edf6ff;--muted:#93a8c3;--line:rgba(102,174,255,.18);--good:#37e6a4;--bad:#ff6b87;--warn:#ffca62;--accent:#43c5ff}
*{box-sizing:border-box}body{margin:0;min-height:100vh;color:var(--ink);background:linear-gradient(180deg,#08111f,var(--bg) 48%);font-family:Manrope,sans-serif}
.site-header{position:sticky;top:0;z-index:20;background:rgba(4,9,18,.94);backdrop-filter:blur(16px);border-bottom:1px solid var(--line)}
.site-header__inner,.wrap{width:min(1280px,100%);margin:auto}.site-header__inner{padding:18px 28px;display:flex;align-items:center;justify-content:space-between;gap:20px}
.site-brand__eyebrow{margin-bottom:6px;color:#14f1ff;font:700 12px/1 'JetBrains Mono',monospace;letter-spacing:.16em;text-transform:uppercase}.site-brand__title{font:700 18px/1.1 Sora,sans-serif}
.site-nav{display:flex;flex-wrap:wrap;gap:10px}.site-nav__link{padding:10px 14px;color:#b8cae3;text-decoration:none;font-weight:600;border:1px solid var(--line);border-radius:8px}.site-nav__link.is-active{color:white;background:rgba(67,197,255,.16);border-color:rgba(67,197,255,.44)}
.wrap{padding:32px 28px 64px}h1,h2,h3,p{margin-top:0}h1,h2,h3{font-family:Sora,sans-serif}h1{font-size:32px}h2{font-size:22px}h3{font-size:16px}
.head{display:flex;justify-content:space-between;gap:20px;padding-bottom:24px;border-bottom:1px solid var(--line)}.head p{max-width:760px;color:var(--muted);line-height:1.6}.stamp{color:var(--muted);font:12px 'JetBrains Mono',monospace;white-space:nowrap}
.section{padding:26px 0;border-bottom:1px solid var(--line)}.section>p{color:var(--muted);line-height:1.6;max-width:900px}.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:18px 0}.card,.notice,.row{border:1px solid var(--line);border-radius:10px;background:rgba(8,17,32,.78)}.card{padding:16px;min-height:112px}.label{color:var(--muted);font-size:12px}.value{margin:9px 0 5px;font:700 25px/1.15 Sora,sans-serif}.note{color:var(--muted);font-size:12px;line-height:1.45}.notice{padding:16px 18px;line-height:1.6;color:#cfdded}.good{color:var(--good)}.bad{color:var(--bad)}
.rows{display:grid;gap:9px;margin-top:15px}.row{display:flex;justify-content:space-between;align-items:center;gap:16px;padding:13px 16px}.row strong{font-size:13px}.row small{display:block;margin-top:4px;color:var(--muted)}.badge{border:1px solid var(--line);border-radius:6px;padding:5px 8px;font-size:11px;font-weight:700;white-space:nowrap}.badge.good{border-color:rgba(55,230,164,.4)}.badge.bad{border-color:rgba(255,107,135,.4)}
.foot{margin-top:24px;color:var(--muted);font-size:12px;line-height:1.6}
@media(max-width:850px){.cards{grid-template-columns:repeat(2,minmax(0,1fr))}.head{display:block}.stamp{margin-top:10px}}
@media(max-width:600px){.site-header__inner{display:block}.site-nav{margin-top:14px}.wrap,.site-header__inner{padding-left:16px;padding-right:16px}.cards{gap:8px}.card{padding:12px}.value{font-size:21px}.row{align-items:flex-start;flex-direction:column}.badge{white-space:normal}}
</style></head><body>__NAV__
<main class="wrap"><header class="head"><div><h1>Исследования стратегии</h1><p>Новая рабочая стратегия — AO/Чайкин. Здесь собраны проверки её исполнения и качества теневого ИИ. Итоги и сделки находятся на основном дашборде.</p></div><div class="stamp" id="stamp">Загрузка…</div></header>
<section class="section"><h2>Исполнение AO/Чайкин</h2><p>Считаем сигналы после перехода: сколько кандидатов стратегия создала, сколько аллокатор выбрал и сколько входов подтвердилось. Так видны потери между сигналом и заявкой.</p><div class="cards" id="executionCards"></div><div id="executionNotice" class="notice"></div><div class="rows" id="recentSignals"></div></section>
<section class="section"><h2>Теневой ИИ: вход или пропуск</h2><p>Проверка через 4 часа показывает только направление движения цены относительно сигнала. Она не измеряет прибыль после комиссии, стопа и размера позиции. Старые прогнозы относятся к часовой стратегии разворота; AO/Чайкин получает отдельную выборку.</p><div id="aiSections"></div></section>
<section class="section"><h2>Гипотеза выхода</h2><p>Единственная оставленная проверка выходов: что было бы при условном удержании ещё двух часовых свечей после сигнала истощения AO. Гипотеза не меняет реальные выходы.</p><div id="exitExperiment" class="notice"></div></section>
<p class="foot">Наблюдения и контрфактические расчёты не являются торговыми сигналами. Решение о настройке правил требует выборки по самой AO/Чайкин стратегии и проверки результата после издержек.</p></main>
<script>
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const pct=v=>v==null?'—':Number(v).toFixed(1)+'%';
const num=v=>Number(v||0).toLocaleString('ru-RU');
const time=v=>{if(!v)return '—';const d=new Date(v);return isNaN(d)?esc(v):d.toLocaleString('ru-RU',{timeZone:'Europe/Moscow',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})};
const card=(label,value,note,cls='')=>'<article class="card"><div class="label">'+esc(label)+'</div><div class="value '+cls+'">'+esc(value)+'</div><div class="note">'+esc(note)+'</div></article>';
function render(data){
 document.getElementById('stamp').textContent='Обновлено '+time(data.generated_at)+' МСК';
 const x=data.execution||{};
 document.getElementById('executionCards').innerHTML=[card('Кандидаты',num(x.candidates),'сигналы AO после перехода'),card('Выбрано',num(x.selected),'решение аллокатора'),card('Вход подтверждён',num(x.confirmed),'по журналу исполнения','good'),card('Без подтверждения',num(x.selected_unconfirmed),'выбранные сигналы')].join('');
 document.getElementById('executionNotice').textContent=x.candidates?'Отложено аллокатором или ограничениями: '+num(x.deferred)+'. Подробные причины смотрите в списке ниже.':'После перехода ещё не было нового кандидата AO. Статистика исполнения появится с первым сигналом.';
 document.getElementById('recentSignals').innerHTML=(x.recent||[]).map(r=>{const confirmed=['confirmed_open','recovered_open'].includes(r.execution_status);const status=confirmed?'ВХОД ПОДТВЕРЖДЁН':r.decision==='selected'?'НЕ ПОДТВЕРЖДЁН':'ОТЛОЖЕН';return '<div class="row"><div><strong>'+esc(r.symbol)+' · '+esc(r.signal)+' · '+time(r.observed_at)+'</strong><small>Аллокатор: '+esc(r.decision||'—')+' · исполнение: '+esc(r.execution_status||'—')+(r.defer_kind?' · '+esc(r.defer_kind):'')+'</small></div><span class="badge '+(confirmed?'good':r.decision==='selected'?'bad':'')+'">'+status+'</span></div>'}).join('');
 const groups=data.ai?.by_strategy||{};let html='';
 for(const [key,title] of [['ao_chaikin_1h','AO / Чайкин — текущая'],['reversal_1h','Часовой разворот — архивная выборка']]){const g=groups[key]||{};html+='<h3>'+title+'</h3><div class="cards">'+card('Проверено',num(g.evaluated),'завершённые наблюдения')+card('ИИ: вход',pct(g.enter_correct_pct),num(g.enter_correct)+' из '+num(g.enter)+' · среднее движение '+pct(g.enter_average_move_pct))+card('ИИ: пропустить',pct(g.abstain_correct_pct),num(g.abstain_correct)+' из '+num(g.abstain)+' · среднее движение '+pct(g.abstain_average_move_pct))+card('Движение по сигналу',pct(g.market_favorable_pct),num(g.market_favorable)+' из '+num(g.evaluated)+' сигналов')+'</div>'}
 document.getElementById('aiSections').innerHTML=html;
 const e=data.exit_experiment||{},ready=e.readiness||{};const count=Number(e.evaluated||0),target=Number(ready.target_evaluated||20);const delta=e.delta_rub_1lot==null?'—':Number(e.delta_rub_1lot).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2})+' ₽/лот';
 document.getElementById('exitExperiment').innerHTML='<strong>'+count+' из '+target+' наблюдений</strong> · '+esc(delta)+' к обычному выходу.<br>'+(ready.status==='ready_for_limited_trial'?'Условия первичной проверки выполнены; нужен отдельный разбор перед изменением правила.':ready.status==='not_confirmed'?'Гипотеза не подтвердилась на текущей выборке.':'Выборка пока мала для вывода.');
}
fetch('/api/strategy-research',{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('HTTP '+r.status);return r.json()}).then(render).catch(e=>{document.getElementById('stamp').textContent='Ошибка загрузки';document.getElementById('executionNotice').textContent=String(e)});
</script></body></html>"""
    return page.replace("__NAV__", site_nav)
