// https://dev.to/ivadyhabimana/setup-tailwind-css-in-a-react-project-configured-from-scratch-a-step-by-step-guide-2jc8
/** @type {import('tailwindcss').Config} */
module.exports = {
    content: ['./src/**/*.{ts,tsx}', './public/index.html'],
    theme: {
        extend: {}
    },
    plugins: []
};
