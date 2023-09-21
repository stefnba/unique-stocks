const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const TsconfigPathsPlugin = require('tsconfig-paths-webpack-plugin');
const NodePolyfillPlugin = require('node-polyfill-webpack-plugin');

module.exports = {
    entry: './src/index.tsx',
    output: {
        filename: 'main.js',
        publicPath: '/',
        path: path.resolve(__dirname, 'build')
    },
    plugins: [
        new HtmlWebpackPlugin({
            template: path.join(__dirname, 'public', 'index.html')
        }),
        new NodePolyfillPlugin()
    ],
    devServer: {
        static: {
            directory: path.join(__dirname, 'build')
        },
        historyApiFallback: {
            index: '/',
            disableDotRule: true
        },
        port: 3000
    },
    module: {
        // exclude node_modules
        rules: [
            {
                // test: /\.(js|jsx|tsx|ts)$/,
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader']
            },
            {
                test: /\.(ts|tsx)$/,
                loader: 'ts-loader'
            },
            {
                test: /\.css$/,
                use: ['style-loader', 'css-loader', 'postcss-loader']
            },
            {
                test: /\.(jpg|png|svg|gif)$/,
                type: 'asset/resource'
            }
        ]
    },
    // pass all js files through Babel
    resolve: {
        extensions: ['.*', '.js', '.jsx', '.ts', '.tsx'],
        plugins: [
            new TsconfigPathsPlugin({
                /* options: see below */
            })
        ]
    }
};
