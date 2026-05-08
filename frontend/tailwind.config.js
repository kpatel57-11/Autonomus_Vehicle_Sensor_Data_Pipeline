/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        'av-bg':       '#050a14',
        'av-card':     '#0d1f38',
        'av-border':   '#1a3a5c',
        'av-cyan':     '#00d4ff',
        'av-green':    '#00ff88',
        'av-amber':    '#ffb800',
        'av-red':      '#ff4060',
        'av-purple':   '#a855f7',
        'av-orange':   '#ff6b35',
      },
      fontFamily: {
        mono: ['Space Mono', 'monospace'],
        sans: ['DM Sans', 'sans-serif'],
      },
    },
  },
  plugins: [],
}
