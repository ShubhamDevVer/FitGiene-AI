/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: 'class',
  content: [
    './templates/**/*.html',
    './apps/**/templates/**/*.html',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      colors: {
        // Brand — Indigo for primary actions
        brand: {
          50:  '#eef2ff',
          100: '#e0e7ff',
          200: '#c7d2fe',
          400: '#818cf8',
          500: '#6366f1',
          600: '#4f46e5',
          700: '#4338ca',
          900: '#312e81',
        },
        // Semantic status
        success: '#10b981',  // emerald-500
        warning: '#f59e0b',  // amber-500
        danger:  '#ef4444',  // red-500
        medical: '#f97316',  // orange-500 — used for health flags
      },
      spacing: {
        // Strict 8px grid
        '4.5': '1.125rem',  // 18px
        '13':  '3.25rem',   // 52px
        '15':  '3.75rem',   // 60px
        '18':  '4.5rem',    // 72px — sidebar width collapsed
        '56':  '14rem',     // 224px — sidebar width expanded
        '88':  '22rem',
      },
      borderRadius: {
        'xl':  '0.75rem',   // 12px — cards
        '2xl': '1rem',      // 16px — modals
        '3xl': '1.25rem',   // 20px — large containers
      },
      fontSize: {
        '2xs': ['0.625rem', { lineHeight: '0.875rem' }],  // 10px labels
        'xs':  ['0.75rem',  { lineHeight: '1rem' }],      // 12px
        'sm':  ['0.875rem', { lineHeight: '1.25rem' }],   // 14px
      },
      boxShadow: {
        'card':  '0 1px 3px 0 rgb(0 0 0 / 0.4), 0 1px 2px -1px rgb(0 0 0 / 0.4)',
        'modal': '0 20px 60px -10px rgb(0 0 0 / 0.6)',
        'glow':  '0 0 0 3px rgb(99 102 241 / 0.3)',
      },
    },
  },
  plugins: [],
}
