import { ThemeProvider } from "@/components/theme-provider";
import { Terminal } from "./components/terminal";

function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <main className="h-screen w-screen flex justify-center items-center">
        <div className="flex w-full h-full items-center justify-center">
          <p>hi</p>
          <Terminal />
        </div>
      </main>
    </ThemeProvider>
  );
}

export default App;
