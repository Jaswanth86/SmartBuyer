import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './styles.css';

const AMAZON_LINK = 'https://link.amazon/B09Y69EFT';
const CERAVE_LINK = 'https://www.cerave.com/skincare/cleansers/foaming-facial-cleanser';

function TiltCard({ children, className = '' }) {
  const ref = useRef(null);
  const [style, setStyle] = useState({});
  const onMove = (e) => {
    const el = ref.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const x = (e.clientX - r.left) / r.width - 0.5;
    const y = (e.clientY - r.top) / r.height - 0.5;
    setStyle({ transform: `perspective(900px) rotateX(${y * -7}deg) rotateY(${x * 9}deg) translateY(-6px)` });
  };
  return <div ref={ref} className={`tilt-card ${className}`} style={style} onMouseMove={onMove} onMouseLeave={() => setStyle({})}>{children}</div>;
}

function Bottle({ large = false }) {
  return <div className={`bottle-scene ${large ? 'large' : ''}`}>
    <div className="aura" />
    <div className="orbit orbit-one" />
    <div className="orbit orbit-two" />
    <div className="bubbles"><i/><i/><i/><i/><i/></div>
    <div className="bottle-shadow" />
    <div className="bottle">
      <div className="pump"><span /></div>
      <div className="cap" />
      <div className="label">
        <strong>CeraVe<span>®</span></strong>
        <small>DEVELOPED WITH DERMATOLOGISTS</small>
        <b>Foaming<br/>Facial<br/>Cleanser</b>
        <em>For Normal to Oily Skin</em>
        <p>Cleanses & removes oil<br/>without disrupting the<br/>protective skin barrier</p>
        <div className="label-line" />
        <small>WITH 3 ESSENTIAL CERAMIDES,<br/>NIACINAMIDE & HYALURONIC ACID</small>
      </div>
    </div>
    <div className="floating-tag tag-top">DERMATOLOGIST<br/><b>DEVELOPED</b></div>
    <div className="floating-tag tag-bottom">SKIN BARRIER<br/><b>FOCUS</b></div>
  </div>
}

function App() {
  const [menu, setMenu] = useState(false);
  const [activeIngredient, setActiveIngredient] = useState('Ceramides');
  useEffect(() => { document.title = 'Smart Buyer Guide — Better choices, beautifully explained.'; }, []);
  const ingredients = {
    Ceramides: 'Help support the skin’s natural protective barrier and help retain moisture.',
    'Hyaluronic Acid': 'A hydrating ingredient that helps attract and retain moisture in the skin.',
    Niacinamide: 'A form of vitamin B3 commonly used to help support a calm-looking skin barrier.'
  };
  return <div className="app">
    <div className="grain" />
    <nav className="nav container">
      <a className="brand" href="#top"><span className="brand-mark">✦</span><span>SMART<br/><b>BUYER GUIDE</b></span></a>
      <div className={`nav-links ${menu ? 'open' : ''}`}>
        <a href="#discover">Discover</a><a href="#reviews">Reviews</a><a href="#ingredients">Ingredients</a><a href="#guides">Guides</a>
      </div>
      <div className="nav-actions"><a className="search" href="#discover">⌕ <span>Search</span></a><a className="nav-cta" href="#featured">Explore <span>↗</span></a><button className="menu-btn" onClick={() => setMenu(!menu)}>{menu ? '×' : '☰'}</button></div>
    </nav>

    <main id="top">
      <section className="hero container">
        <div className="hero-copy">
          <div className="eyebrow"><span className="pulse" /> PRODUCT INTELLIGENCE / 001</div>
          <h1>Better choices.<br/><span>Beautifully</span><br/>explained.</h1>
          <p className="hero-text">A smarter way to discover products worth your attention. Independent-feeling guides, useful details, and no unnecessary noise.</p>
          <div className="hero-actions"><a className="button primary" href="#featured">Explore the guide <span>↗</span></a><a className="text-link" href="#reviews">See our latest review <span>↓</span></a></div>
          <div className="hero-meta"><div><b>01</b><span>Research-led<br/>discoveries</span></div><div><b>∞</b><span>Designed for<br/>curious buyers</span></div></div>
        </div>
        <div className="hero-visual"><div className="visual-label label-left">01 / FEATURED<br/><b>FORMULA STUDY</b></div><Bottle large/><div className="score-card"><span>SMART SCORE</span><strong>8.7<span>/10</span></strong><small>Thoughtful daily cleansing</small><div className="score-bar"><i /></div></div><div className="scroll-cue">SCROLL TO DISCOVER <span>↓</span></div></div>
      </section>

      <section className="ticker"><div className="ticker-track"><span>CURATED DISCOVERIES</span><i>✦</i><span>HONEST DETAILS</span><i>✦</i><span>SMARTER BUYING</span><i>✦</i><span>CURATED DISCOVERIES</span><i>✦</i><span>HONEST DETAILS</span></div></section>

      <section className="section container" id="featured">
        <div className="section-head"><div><div className="eyebrow">01 / FEATURED DISCOVERY</div><h2>A closer look at<br/><span>what matters.</span></h2></div><p>We turn product pages into clear, useful experiences—so you can spend less time searching and more time choosing well.</p></div>
        <TiltCard className="feature-card"><div className="feature-image"><div className="mini-grid"/><Bottle/></div><div className="feature-content"><div className="product-kicker">SKINCARE / DAILY CLEANSER</div><h3>CeraVe Foaming<br/>Facial Cleanser</h3><p>A gentle, foaming cleanser designed for normal to oily skin. A formula study focused on cleansing, oil removal, and maintaining the skin barrier.</p><div className="feature-tags"><span>Normal → oily skin</span><span>Foaming texture</span><span>Barrier-minded</span></div><div className="feature-bottom"><div className="review-stars">★★★★★ <small>Editor's research pick</small></div><a className="button primary" href={AMAZON_LINK} target="_blank" rel="noreferrer">Check Amazon <span>↗</span></a></div><a className="source-link" href={CERAVE_LINK} target="_blank" rel="noreferrer">Read the brand’s product details ↗</a></div></TiltCard>
      </section>

      <section className="dark-section" id="ingredients"><div className="container ingredient-layout"><div className="ingredient-copy"><div className="eyebrow mint">02 / FORMULA UNPACKED</div><h2>Small ingredients.<br/><span>Big context.</span></h2><p>Good buying decisions start with understanding what a product is designed to do. Explore the key ingredients highlighted for this cleanser.</p><div className="ingredient-tabs">{Object.keys(ingredients).map(name => <button key={name} className={activeIngredient === name ? 'active' : ''} onClick={() => setActiveIngredient(name)}>{name}<span>↗</span></button>)}</div><div className="ingredient-detail"><div className="detail-number">0{Object.keys(ingredients).indexOf(activeIngredient) + 1}</div><div><h4>{activeIngredient}</h4><p>{ingredients[activeIngredient]}</p></div></div></div><div className="molecule-stage"><div className="molecule-ring ring-a"/><div className="molecule-ring ring-b"/><div className="molecule-core"><span>FORMULA<br/><b>LAB</b></span></div><div className="molecule-node node-a">CERA</div><div className="molecule-node node-b">HA</div><div className="molecule-node node-c">B3</div><div className="stage-caption">ACTIVE SYSTEM / 03<br/><b>ESSENTIAL COMPONENTS</b></div></div></div></section>

      <section className="section container" id="reviews"><div className="section-head compact"><div><div className="eyebrow">03 / THE SMART VERDICT</div><h2>Useful clarity,<br/><span>not hype.</span></h2></div><p>Our review format separates what the product says, who it may suit, and what to consider before buying.</p></div><div className="verdict-grid"><TiltCard className="verdict-main"><div className="verdict-top"><span>EDITORIAL TAKE</span><span className="verified">● RESEARCHED</span></div><h3>A practical cleanser<br/>for a considered routine.</h3><p>Its positioning is straightforward: cleanse away excess oil while keeping the skin’s protective barrier in mind. Always consider your own skin needs and sensitivities.</p><div className="verdict-score"><strong>8.7</strong><div><span>SMART SCORE</span><small>Based on formula positioning, usability, and everyday relevance.</small></div></div></TiltCard><div className="verdict-list"><div><span>BEST FOR</span><b>Normal to oily skin</b><small>A foaming format for shoppers looking for a daily cleanser.</small></div><div><span>LOOK FOR</span><b>Ceramides + hydration</b><small>Ingredients highlighted by the brand for barrier support.</small></div><div><span>REMEMBER</span><b>Your skin is personal</b><small>Patch test and stop use if irritation occurs.</small></div></div></div></section>

      <section className="guide-section" id="guides"><div className="container"><div className="section-head light"><div><div className="eyebrow mint">04 / THE GUIDE INDEX</div><h2>Go deeper.<br/><span>Buy smarter.</span></h2></div><a className="button outline" href="#top">View all guides <span>↗</span></a></div><div className="guide-grid"><a className="guide-card" href="#ingredients"><span>01</span><h3>How to choose<br/>a cleanser</h3><small>FOUNDATION / 06 MIN READ ↗</small></a><a className="guide-card featured-guide" href="#featured"><span>02</span><h3>Ingredient<br/>decoder</h3><small>SKINCARE / 04 MIN READ ↗</small></a><a className="guide-card" href="#reviews"><span>03</span><h3>How we score<br/>a product</h3><small>METHOD / 03 MIN READ ↗</small></a></div></div></section>
    </main>

    <footer className="footer"><div className="container footer-top"><a className="brand" href="#top"><span className="brand-mark">✦</span><span>SMART<br/><b>BUYER GUIDE</b></span></a><div className="footer-note">Thoughtful discoveries for<br/>the modern buyer.</div><div className="footer-links"><a href="#featured">Discover</a><a href="#reviews">Reviews</a><a href="#guides">Guides</a><a href={CERAVE_LINK} target="_blank" rel="noreferrer">Sources ↗</a></div></div><div className="container footer-bottom"><span>© 2026 SMART BUYER GUIDE</span><span>Built for better decisions.</span><span>Affiliate links may earn a commission.</span></div></footer>
  </div>;
}

createRoot(document.getElementById('root')).render(<App />);
