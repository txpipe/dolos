export default {
  logo: <h1 className="font-bold text-4xl md:text-4xl lg:text-5xl">Dolos</h1>,
  project: {
    link: "https://github.com/txpipe/dolos",
  },
  chat: {
    link: "https://discord.gg/Vc3x8N9nz2",
  },
  footer: {
    text: "Dolos - TxPipe",
  },
  nextThemes: {
    defaultTheme: "dark",
  },
  docsRepositoryBase: "https://github.com/txpipe/dolos/tree/main/docs",
  useNextSeoProps() {
    return {
      titleTemplate: "%s – Dolos",
      description: "A Cardano data node in Rust",
      canonical: "https://dolos.txpipe.io",
      siteName: "Dolos",
      openGraph: {
        url: "https://dolos.txpipe.io",
        title: "Dolos",
        description: "A Cardano data node in Rust",
        images: [
          {
            url: "https://dolos.txpipe.io/logo.webp",
            width: 732,
            height: 287,
            alt: "Dolos",
            type: "image/webp",
          },
        ],
      },
    };
  },
};
