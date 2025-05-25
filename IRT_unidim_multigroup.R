# ------------------------------------------------------------------------------
# Script: IRT_unidim.R
# Descrição: Interface gráfica para análise IRT unidimensional com validação INEP.
#            Permite a seleção de arquivos, códigos de prova e modelos IRT (1PL/2PL/3PL),
#            executa a estimação de parâmetros e compara os resultados com os valores
#            oficiais do INEP para validação de precisão.
# Autor: InterLiner
# Data: 2023
# ------------------------------------------------------------------------------

# Configurar locale para UTF-8
# Sys.setlocale("LC_ALL", "Portuguese_Brazil.1252")

# Carregar bibliotecas necessárias
suppressMessages(library(data.table))
suppressMessages(library(arrow))
suppressMessages(library(mirt))
suppressMessages(library(tcltk))
suppressMessages(library(parallel))

# Definir interface para seleção de arquivo e código de prova
IRT_unidim <- function() {
  # Criar janela principal
  win <- tktoplevel()
  tktitle(win) <- "IRT Unidimensional - Validação com INEP (com Verificação de Gabaritos)"
  tkwm.deiconify(win)
  tkfocus(win)
  
  # Variáveis para armazenar valores
  input_file <- tclVar("C:/code/interliner/data/parquet/ch_1195_cn_1225_mt_1215_lc_1205.parquet")
  selected_prova <- tclVar("1195") # Valor padrão inicial (exemplo de código comum)
  output_dir <- tclVar(getwd())
  model_type <- tclVar("3PL")
  
  # Variáveis para parâmetros avançados
  estimation_method <- tclVar("MCEM") # EM, MCEM, QMCEM
  max_iter_var <- tclVar("10") # Número máximo de iterações
  tol_var <- tclVar("0.01") # Tolerância para convergência
  quadpts_var <- tclVar("21") # Pontos de quadratura para integração numérica
  theta_min_var <- tclVar("-5") # Limite inferior de theta
  theta_max_var <- tclVar("5") # Limite superior de theta
  cores_var <- tclVar(as.character(max(1, parallel::detectCores() - 1))) # Número de cores para processamento paralelo

  # Função para selecionar arquivo de entrada
  select_input_file <- function() {
    file_path <- tclvalue(tkgetOpenFile(filetypes = "{{Parquet Files} {.parquet}} {{All Files} {*}}"))
    if (file_path != "") {
      tclvalue(input_file) <- file_path
      # Carregar lista de códigos de prova disponíveis
      load_available_codes(file_path)
    }
  }
  
  # Função para carregar códigos de prova disponíveis
  # Carregar dataset09 (ENEM 2009) apenas uma vez e deixar global
  if (!exists("dataset09", envir = .GlobalEnv)) {
    tryCatch({
      assign("dataset09", arrow::read_parquet("B:/Corpus/enem/microdados_enem_2009/DADOS/MICRODADOS_ENEM_2009s.parquet"), envir = .GlobalEnv)
      cat("[INFO] Arquivo ENEM 2009 carregado em dataset09.\n")
    }, error=function(e) {
      cat("[ERRO] Falha ao carregar ENEM 2009: ", e$message, "\n")
      assign("dataset09", NULL, envir = .GlobalEnv)
    })
  }

  load_available_codes <- function(file_path) {
    tryCatch({
      cat(sprintf("[INFO] Carregando arquivo: %s\n", file_path))
      dataset <- arrow::read_parquet(file_path)

      if ("CO_PROVA" %in% names(dataset)) {
        provas <- sort(unique(dataset$CO_PROVA))
        cat(sprintf("[INFO] Códigos de prova disponíveis: %s\n", paste(provas, collapse=", ")))
        
        # Mostrar os códigos disponíveis no console
        cat(sprintf("[INFO] Códigos de prova disponíveis: %s\n", paste(provas, collapse=", ")))
        
        # Definir primeiro código como padrão no campo de texto
        if (length(provas) > 0) {
          tclvalue(selected_prova) <- as.character(provas[1])
        }
      } else {
        cat("[AVISO] Coluna CO_PROVA não encontrada no arquivo.\n")
      }
    }, error = function(e) {
      cat(sprintf("[ERRO] Falha ao carregar arquivo: %s\n", e$message))
    })
  }
  
  # Função para selecionar diretório de saída
  select_output_dir <- function() {
    dir_path <- tclvalue(tkchooseDirectory())
    if (dir_path == "") dir_path <- tclvalue(tkgetSaveFile())
    if (dir_path != "") {
      tclvalue(output_dir) <- dir_path
    }
  }
  
  # Função para processar os dados
  process_data <- function() {
    file_path <- tclvalue(input_file)
    prova_code <- as.integer(tclvalue(selected_prova))
    output_dir_value <- tclvalue(output_dir)
    model <- tclvalue(model_type)
    
    if (file_path == "") {
      tkmessageBox(title="Erro", message="Selecione um arquivo de entrada.", icon="error")
      return()
    }
    
    if (is.na(prova_code)) {
      tkmessageBox(title="Erro", message="Selecione um código de prova válido.", icon="error")
      return()
    }
    
    # Carregar dados
    cat(sprintf("[INFO] Carregando arquivo: %s\n", file_path))
    dataset <- arrow::read_parquet(file_path)
    
    # Verificar se dataset09 está disponível
    if (!exists("dataset09", envir = .GlobalEnv) || is.null(get("dataset09", envir = .GlobalEnv))) {
      tkmessageBox(title="Erro", message="Arquivo ENEM 2009 não carregado! Verifique o caminho.", icon="error")
      return()
    }
    dataset09 <- get("dataset09", envir = .GlobalEnv)
    
    # Usar o código de prova digitado pelo usuário, sem tentar filtrar o dataset
    cat(sprintf("[INFO] Usando código de prova digitado: %d\n", prova_code))
    dataset_filtered <- dataset
    
    # Função para verificar a correspondência entre código de prova e área
    validate_prova_area <- function(dataset, prova_code, area_code) {
      cat("\n[INFO] Validando correspondência entre código de prova e área...\n")
      
      # Mapear possíveis áreas nos códigos de colunas
      areas <- c("CH", "CN", "LC", "MT")
      found_areas <- list()
      
      # Verificar quais áreas têm colunas de resposta/gabarito no dataset
      for (area in areas) {
        resp_col <- paste0("TX_RESPOSTAS_", area)
        gab_col <- paste0("TX_GABARITO_", area)
        
        if (resp_col %in% names(dataset) && gab_col %in% names(dataset)) {
          # Contar quantos registros não-NA existem para cada área
          n_valid <- sum(!is.na(dataset[[resp_col]]))
          found_areas[[area]] <- n_valid
          cat(sprintf("[INFO]   - %s: %d registros válidos encontrados\n", area, n_valid))
        }
      }
      
      # Verificar se temos a coluna CO_PROVA_XX para a área específica
      prova_col <- paste0("CO_PROVA_", area_code)
      has_co_prova <- prova_col %in% names(dataset)
      
      if (has_co_prova) {
        # Mostrar distribuição de códigos de prova no dataset para esta área
        prova_dist <- table(dataset[[prova_col]])
        cat(sprintf("[INFO] Distribuição de códigos de prova para %s no dataset:\n", area_code))
        for (code in names(prova_dist)) {
          cat(sprintf("[INFO]   - Prova %s: %d registros\n", code, prova_dist[code]))
        }
        
        # Verificar se o código escolhido existe no dataset
        if (as.character(prova_code) %in% names(prova_dist)) {
          cat(sprintf("[INFO] Código de prova %s CONFIRMADO no dataset para área %s (%d registros)\n", 
                    prova_code, area_code, prova_dist[as.character(prova_code)]))
        } else {
          cat(sprintf("[AVISO] Código de prova %s NÃO ENCONTRADO no dataset para área %s! \n", prova_code, area_code))
          cat("[AVISO] Isto pode causar erros na comparação com dados do INEP.\n")
        }
      } else {
        cat(sprintf("[INFO] Coluna %s não encontrada. Validação cruzada com código não possível.\n", prova_col))
      }
      
      # Retornar resumo da validação
      prova_exists <- FALSE
      if (has_co_prova) {
        prova_exists <- as.character(prova_code) %in% names(table(dataset[[prova_col]]))
      }
      
      return(list(
        found_areas = found_areas,
        has_co_prova = has_co_prova,
        prova_exists = prova_exists
      ))
    }
    
    # Função para determinar a área a partir do código de prova
    get_area_from_code <- function(code) {
      # Baseado na observação dos arquivos e padrões do ENEM
      # Intervalo mais preciso
      
      # Códigos padrão do ENEM por área (ajustar conforme necessidade)
      ch_codes <- c(1195, 1190, 1185, 1180, 1175, 1170, 1165, 1160, 1155, 1150, 1145, 1140)
      cn_codes <- c(1225, 1220, 1215, 1210, 1205, 1200, 1195, 1190, 1185, 1180, 1175, 1170)
      mt_codes <- c(1215, 1210, 1205, 1200, 1195, 1190, 1185, 1180, 1175, 1170, 1165, 1160)
      lc_codes <- c(1205, 1200, 1195, 1190, 1185, 1180, 1175, 1170, 1165, 1160, 1155, 1150)
      
      # Verificar correspondência exata primeiro
      if (code %in% ch_codes) {
        return("CH")
      } else if (code %in% cn_codes) {
        return("CN")
      } else if (code %in% mt_codes) {
        return("MT")
      } else if (code %in% lc_codes) {
        return("LC")
      }
      
      # Se não for um código exato, usar a heurística por faixa (menos precisa)
      if (code >= 1000 && code < 1200) {
        return("CH")
      } else if (code >= 1200 && code < 1400) {
        return("CN")
      } else if (code >= 1400 && code < 1600) {
        return("MT")
      } else if (code >= 1600 && code < 1800) {
        return("LC")
      } else {
        return(NA)
      }
    }
    
    # Executar validação de correspondência entre código de prova e área
    # Obter a área para validação inicial
    temp_area_code <- get_area_from_code(prova_code)
    validation_result <- validate_prova_area(dataset, prova_code, temp_area_code)
    
    # Identificar área da prova a partir do código
    area_code <- get_area_from_code(prova_code)
    
    if (is.na(area_code)) {
      cat(sprintf("[ERRO] Não foi possível identificar a área para o código de prova %d\n", prova_code))
      tkmessageBox(title = "Erro", message = sprintf("Código de prova %d fora dos intervalos esperados", prova_code), icon = "error")
      return()
    }
    
    cat(sprintf("[INFO] Área da prova identificada: %s\n", area_code))
    
    # Validar a correspondência entre o código de prova e a área
    validation <- validate_prova_area(dataset, prova_code, area_code)
    
    # Alerta se o código de prova não for encontrado no dataset
    if (validation$has_co_prova && !validation$prova_exists) {
      result <- tkmessageBox(title = "Aviso", message = sprintf(
        "O código de prova %d não foi encontrado no dataset.\nDeseja continuar mesmo assim?", prova_code), 
        icon = "warning", type = "yesno")
      if (tclvalue(result) == "no") {
        return()
      }
    }
    
    # Procurar colunas com o sufixo correto
    tx_respostas_col <- paste0("TX_RESPOSTAS_", area_code)
    tx_gabarito_col <- paste0("TX_GABARITO_", area_code)
    
    if (!(tx_respostas_col %in% names(dataset_filtered) && tx_gabarito_col %in% names(dataset_filtered))) {
      cat(sprintf("[ERRO] Arquivo não contém as colunas obrigatórias: %s, %s\n", tx_respostas_col, tx_gabarito_col))
      tkmessageBox(title="Erro", message=sprintf("Arquivo não contém as colunas obrigatórias: %s, %s", tx_respostas_col, tx_gabarito_col), icon="error")
      return(NULL)
    }
    
    # Chamar análise principal
    # Passar dataset corrente e dataset09 para análise multi-group
    result <- run_irt_analysis(dataset_filtered, dataset09, model, tx_respostas_col, tx_gabarito_col, area_code)
    
    # Comparar com dados do INEP somente se tivermos resultados
    if (!is.null(result)) {
      cat("\n[INFO] Comparando resultados com dados oficiais do INEP...\n")
      inep_results <- compare_with_inep(result, prova_code, area_code)
{{ ... }}
      
      # Mostrar um resumo da qualidade da estimativa
      if (!is.null(inep_results)) {
        error_levels <- list(
          low = sum(inep_results$error_levels == "baixo"),
          medium = sum(inep_results$error_levels == "médio"),
          high = sum(inep_results$error_levels == "alto")
        )
        
        total_params <- nrow(inep_results)
        if (total_params > 0) {
          cat(sprintf("\n[RESUMO] Qualidade da Estimativa (total: %d parâmetros):\n", total_params))
          cat(sprintf("  - Erro baixo (<10%%): %d (%.1f%%)\n", 
           error_levels$low, 100 * error_levels$low / total_params))
          cat(sprintf("  - Erro médio (10-25%%): %d (%.1f%%)\n", 
           error_levels$medium, 100 * error_levels$medium / total_params))
          cat(sprintf("  - Erro alto (>25%%): %d (%.1f%%)\n", 
           error_levels$high, 100 * error_levels$high / total_params))
          
          # Sugestão de melhorias para o usuário
          if (error_levels$high > error_levels$low) {
            cat("\n[SUGESTÕES] Para melhorar a precisão, considere:\n")
            cat("  - Verificar se o código de prova corresponde à área correta\n")
            cat("  - Aumentar o número de iterações (>1000)\n")
            cat("  - Ajustar o método de estimação (EM, MCEM, QMCEM)\n")
            cat("  - Aumentar os pontos de quadratura (>31)\n")
          }
        }
      }
    }
  }
  
  # Função para executar a análise IRT
  run_irt_analysis <- function(data_cur, data_ref, model_type, tx_respostas_col, tx_gabarito_col, area_code, sample_ref = 1e5) {
    # Multi-Group Equating Implementation
    start_time <- Sys.time()
    cat("[INFO] Iniciando análise IRT Multi-Group (Equating)...\n")
    
    # Obter parâmetros da interface
    method <- tclvalue(estimation_method)
    max_iter <- as.integer(tclvalue(max_iter_var))
    tol <- as.numeric(tclvalue(tol_var))
    quadpts <- as.integer(tclvalue(quadpts_var))
    theta_min <- as.numeric(tclvalue(theta_min_var))
    theta_max <- as.numeric(tclvalue(theta_max_var))
    num_cores <- as.integer(tclvalue(cores_var))
    
    cat(sprintf("[INFO] Usando método: %s, max_iter: %d, tol: %.5f\n", method, max_iter, tol))
    cat(sprintf("[INFO] Quadratura: %d pontos, theta range: [%.1f, %.1f]\n", quadpts, theta_min, theta_max))
    cat(sprintf("[INFO] Paralelismo: %d cores\n", num_cores))
    
    # --- Preprocessar ambos os conjuntos ---
    cat("[INFO] Preprocessando grupo de referência (2009)...\n")
    # Opcional: Amostrar até 100.000 respondentes do grupo de referência
    if (!is.null(sample_ref) && nrow(data_ref) > sample_ref) {
      set.seed(42)
      idx <- sample(seq_len(nrow(data_ref)), sample_ref)
      data_ref <- data_ref[idx, ]
      cat(sprintf("[INFO] Amostrando %d de %d respondentes do grupo de referência\n", sample_ref, nrow(data_ref)))
    }
    R09 <- prepare_response_matrix(data_ref[[tx_respostas_col]], data_ref[[tx_gabarito_col]])
    rows_to_keep09 <- apply(R09, 1, function(row) sum(!is.na(row)) > 0)
    R09 <- R09[rows_to_keep09, ]
    cat(sprintf("[INFO] Grupo referência: %d alunos x %d itens\n", nrow(R09), ncol(R09)))
    
    cat("[INFO] Preprocessando grupo atual (edição corrente)...\n")
    R23 <- prepare_response_matrix(data_cur[[tx_respostas_col]], data_cur[[tx_gabarito_col]])
    rows_to_keep23 <- apply(R23, 1, function(row) sum(!is.na(row)) > 0)
    R23 <- R23[rows_to_keep23, ]
    cat(sprintf("[INFO] Grupo atual: %d alunos x %d itens\n", nrow(R23), ncol(R23)))
    
    # --- Combinar respostas e definir grupos ---
    all_resp <- rbind(R09, R23)
    grp <- factor(c(rep("ref", nrow(R09)), rep("cur", nrow(R23))))
    common <- 1:ncol(all_resp) # todos os itens são comuns
    
    # Definir nomes únicos para as colunas (itens)
    colnames(all_resp) <- paste0(area_code, "_item_", sprintf("%02d", seq_len(ncol(all_resp))))
    
    # --- Configuração do modelo ---
    model_spec <- switch(
      model_type,
      "1PL" = "Rasch",
      "2PL" = "2PL",
      "3PL" = "3PL",
      "3PL" # padrão
    )
    technical_opts <- list()
    technical_opts$NCYCLES <- max_iter
    
    if (num_cores > 1) {
      cat(sprintf("[INFO] Ativando processamento paralelo com %d cores\n", num_cores))
      use_parallel <- num_cores
    } else {
      use_parallel <- FALSE
    }
    
    # --- Multi-Group Estimation ---
    start_estimation <- Sys.time()
    cat("[INFO] Estimando modelo Multi-Group (multipleGroup)...\n")
    
    # Configurar invariance de acordo com o modelo
    inv_params <- c("free_means", "free_var")
    if (model_spec == "3PL") {
      inv_params <- c("slopes", "intercepts", "guess", "free_means", "free_var")
    } else if (model_spec == "2PL") {
      inv_params <- c("slopes", "intercepts", "free_means", "free_var")
    } else if (model_spec == "Rasch") {
      inv_params <- c("intercepts", "free_means", "free_var")
    }
    
    cat(sprintf("[INFO] Usando invariance: %s\n", paste(inv_params, collapse=", ")))
    
    model <- multipleGroup(
      all_resp, 1,
      itemtype  = model_spec,
      group     = grp,
      invariance = inv_params,
      D        = 1.7,
      quadpts  = quadpts,
      TOL      = tol,
      technical = technical_opts,
      method    = method,
      parallel  = use_parallel
    )
    end_estimation <- Sys.time()
    estimation_time <- difftime(end_estimation, start_estimation, units = "secs")
    cat(sprintf("[INFO] Estimação concluída em %.2f segundos\n", as.numeric(estimation_time)))
    
    # --- Parâmetros ---
    cat("[INFO] Extraindo parâmetros do modelo...\n")
    params_list <- coef(model, simplify=TRUE)
    
    # Verificar a estrutura dos parâmetros
    if (is.null(params_list) || !is.list(params_list)) {
      stop("[ERRO] Falha ao extrair parâmetros do modelo!")
    }
    
    # Extrair parâmetros do grupo de referência (2009)
    if ("ref" %in% names(params_list)) {
      cat("[INFO] Usando parâmetros do grupo de referência (2009)\n")
      params <- params_list$ref
    } else if ("items" %in% names(params_list)) {
      cat("[INFO] Usando parâmetros comuns\n")
      params <- params_list$items
    } else {
      stop("[ERRO] Estrutura de parâmetros inesperada!")
    }
    
    # Mostrar colunas disponíveis para diagnóstico
    if (is.matrix(params) || is.data.frame(params)) {
      cat(sprintf("[INFO] Colunas disponíveis: %s\n", paste(colnames(params), collapse=", ")))
    } else {
      cat("[INFO] Parâmetros em formato de lista\n")
    }
    
    # Verificar estrutura dos parâmetros e extrair corretamente
    cat("[INFO] Extraindo parâmetros do modelo...\n")
    cat(sprintf("[INFO] Colunas disponíveis: %s\n", paste(colnames(params), collapse=", ")))
    
    # --- Resultados ---
    # Preparar dataframe base
    result <- data.frame(
      domain = area_code,
      item = paste0(area_code, "_item_", sprintf("%02d", seq_len(ncol(all_resp)))),
      model = model_type,
      students = nrow(all_resp)
    )
    
    # Extrair parâmetros a, b, c com tratamento de erros
    if (is.matrix(params) || is.data.frame(params)) {
      # Parâmetro a (discriminação)
      if ("a1" %in% colnames(params)) {
        result$a <- as.numeric(params[, "a1"])
      } else if (model_spec == "Rasch") {
        result$a <- rep(1.0, nrow(result))
      } else {
        cat("[AVISO] Coluna 'a1' não encontrada, usando valores padrão\n")
        result$a <- rep(1.0, nrow(result))
      }
      
      # Parâmetro b (dificuldade)
      if ("d" %in% colnames(params)) {
        if ("a1" %in% colnames(params) && model_spec != "Rasch") {
          result$b <- -as.numeric(params[, "d"]) / as.numeric(params[, "a1"])
        } else {
          result$b <- -as.numeric(params[, "d"])  # Para Rasch
        }
      } else {
        cat("[AVISO] Coluna 'd' não encontrada, usando valores padrão\n")
        result$b <- rep(0, nrow(result))
      }
      
      # Parâmetro c (adivinhação)
      if ("g" %in% colnames(params) && model_spec == "3PL") {
        result$c <- as.numeric(params[, "g"])
      } else {
        result$c <- rep(0, nrow(result))
      }
    } else {
      stop("[ERRO] Formato de parâmetros não suportado!")
    }
    
    # Extrair gabaritos
    first_valid_gabarito <- NA
    if (!is.null(data_ref[[tx_gabarito_col]])) {
      first_valid_gabarito <- data_ref[[tx_gabarito_col]][!is.na(data_ref[[tx_gabarito_col]])][1]
    } else if (!is.null(data_cur[[tx_gabarito_col]])) {
      first_valid_gabarito <- data_cur[[tx_gabarito_col]][!is.na(data_cur[[tx_gabarito_col]])][1]
      # Mapear parâmetros conforme o modelo
      a_val <- params[i, "a1"]
      if (model_type == "1PL") {
        a_val <- 1.0  # No modelo Rasch, a é fixo em 1
      }
      
      # Para modelos com 3 parâmetros, mapear diretamente
      if (model_type == "3PL") {
        a_val <- params[i, "a1"]
        b_val <- -params[i, "d"] / params[i, "a1"]  # b = -d/a
        c_val <- params[i, "g"]  # c = g (guess parameter)
      } else if (model_type == "2PL") {
        a_val <- params[i, "a1"]
        b_val <- -params[i, "d"] / params[i, "a1"]
        c_val <- 0  # No modelo 2PL, c é fixo em 0
      } else {  # 1PL/Rasch
        a_val <- 1.0
        b_val <- -params[i, "d"]  # No Rasch, b = -d porque a=1
        c_val <- 0  # No modelo 1PL, c é fixo em 0
      }
      
      # Extrair gabarito do item atual
      # Usamos o primeiro caractere do gabarito na posição correspondente ao item
      gabarito_val <- ""
      if (!is.null(data[[tx_gabarito_col]])) {
        first_valid_gabarito <- data[[tx_gabarito_col]][!is.na(data[[tx_gabarito_col]])][1]
        if (!is.na(first_valid_gabarito) && nchar(first_valid_gabarito) >= i) {
          gabarito_val <- substr(first_valid_gabarito, i, i)
        }
      }
      
      # Armazenar no dataframe de resultados
      result$domain[i] <- domain_code
      result$item[i] <- item_num
      result$a[i] <- a_val
      result$b[i] <- b_val
      result$c[i] <- c_val
      result$gabarito[i] <- gabarito_val
    }
    
    # Adicionar metadados ao resultado
    result$CO_PROVA <- prova_code
    result$SG_AREA <- area_code
    result$TX_MODELO <- model_spec
    result$DT_ESTIMACAO <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    result$NR_ITER <- max_iter
    result$NR_QUADPTS <- quadpts
    result$TX_METHOD <- method
    result$NR_THETA_MIN <- theta_min
    result$NR_THETA_MAX <- theta_max
    result$NR_CORES <- num_cores
    
    end_time <- Sys.time()
    execution_time <- difftime(end_time, start_time, units = "mins")
    
    # Corrigir problemas de encoding no console
    try({
      if (.Platform$OS.type == "windows") {
        Sys.setlocale("LC_CTYPE", "Portuguese_Brazil.1252")
      }
    }, silent=TRUE)
    
    cat(sprintf("[INFO] Tempo de execução: %.2f minutos\n", as.numeric(execution_time)))
    cat(sprintf("Estimados parâmetros Multi-Group para %d itens usando modelo %s\n\n", ncol(all_resp), model_type))
    
    # Mostrar estatísticas dos parâmetros
    show_parameter_stats(result)
    
    # Salvar resultado em formato Parquet
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    output_file <- file.path(tclvalue(output_dir), 
                           sprintf("irt_unidim_%s_prova%d_%s.parquet", area_code, prova_code, timestamp))
    
    cat(sprintf("[INFO] Salvando resultados em: %s\n", output_file))
    arrow::write_parquet(result, output_file)
    cat(sprintf("Resultado salvo com sucesso em formato Parquet!\n"))
    
    # Disponibilizar no ambiente global
    assign("irt_result", result, envir = .GlobalEnv)
    cat("Resultado também disponibilizado na variável 'irt_result'\n")
    
    return(result)
  }
  
  # Função para preparar matriz de respostas
  prepare_response_matrix <- function(respostas, gabarito) {
    cat("[INFO] Preparando matriz de respostas...\n")
    n_students <- length(respostas)
    
    # Determinar o número de itens com base no primeiro gabarito
    if (n_students == 0) {
      cat("[ERRO] Nenhuma resposta encontrada!\n")
      return(NULL)
    }
    
    first_key <- gabarito[1]
    n_items <- nchar(first_key)
    cat(sprintf("[INFO] Número de itens detectado: %d\n", n_items))
    
    # Criar matriz vazia
    matrix_data <- matrix(NA, nrow = n_students, ncol = n_items)
    
    # Preencher a matriz
    for (i in seq_len(n_students)) {
      resp_str <- respostas[i]
      key_str <- gabarito[i]
      
      # Verificar se o comprimento da resposta é igual ao do gabarito
      if (nchar(resp_str) != nchar(key_str)) {
        cat(sprintf("[AVISO] Estudante %d: Comprimento da resposta (%d) difere do gabarito (%d)\n", 
                   i, nchar(resp_str), nchar(key_str)))
        next
      }
      
      # Converter para vetor de caracteres
      resp_vec <- strsplit(resp_str, "")[[1]]
      key_vec <- strsplit(key_str, "")[[1]]
      
      # Comparar e preencher a matriz (1 = correto, 0 = incorreto)
      for (j in 1:length(resp_vec)) {
        if (j <= n_items) {
          if (resp_vec[j] == "9" || resp_vec[j] == "." || resp_vec[j] == " ") {
            # Tratar como NA (missing)
            matrix_data[i, j] <- NA
          } else if (resp_vec[j] == key_vec[j]) {
            # Resposta correta
            matrix_data[i, j] <- 1
          } else {
            # Resposta incorreta
            matrix_data[i, j] <- 0
          }
        }
      }
    }
    
    # Verificar dados faltantes
    missing_count <- sum(is.na(matrix_data))
    total_cells <- prod(dim(matrix_data))
    missing_percentage <- 100 * missing_count / total_cells
    
    cat(sprintf("[INFO] Dados faltantes: %d (%.2f%%)\n", missing_count, missing_percentage))
    
    return(matrix_data)
  }
  
  # Função para mostrar estatísticas dos parâmetros
  show_parameter_stats <- function(result) {
    # Verificar valores NA
    na_count <- sum(is.na(result$a)) + sum(is.na(result$b)) + sum(is.na(result$c))
    if (na_count > 0) {
      cat(sprintf("[AVISO] Valores NA detectados: a=%d, b=%d, c=%d\n", 
                sum(is.na(result$a)), sum(is.na(result$b)), sum(is.na(result$c))))
    }
    
    # Calcular estatísticas
    a_range <- range(result$a, na.rm=TRUE)
    a_mean <- mean(result$a, na.rm=TRUE)
    
    b_range <- range(result$b, na.rm=TRUE)
    b_mean <- mean(result$b, na.rm=TRUE)
    
    c_range <- range(result$c, na.rm=TRUE)
    c_mean <- mean(result$c, na.rm=TRUE)
    
    cat(sprintf("Parâmetro a (discriminação): %.3f a %.3f, média=%.3f\n", a_range[1], a_range[2], a_mean))
    cat(sprintf("Parâmetro b (dificuldade): %.3f a %.3f, média=%.3f\n", b_range[1], b_range[2], b_mean))
    cat(sprintf("Parâmetro c (adivinhação): %.3f a %.3f, média=%.3f\n", c_range[1], c_range[2], c_mean))
  }
  
  # Função para comparar com dados do INEP
  compare_with_inep <- function(result, prova_code, area_code) {
    tryCatch({
      cat("\n[INFO] Comparando com dados oficiais do INEP...\n")
      
      # Procurar arquivo INEP para o código de prova
      inep_file <- "C:/code/interliner/data/parquet/ITENS_PROVA_2023.csv"
      if (!file.exists(inep_file)) {
        cat(sprintf("[AVISO] Arquivo de parâmetros INEP não encontrado: %s\n", inep_file))
        inep_file <- tk_choose.files(title="Selecione o arquivo de parâmetros INEP", 
                                   filetypes="{{Parquet Files} {.parquet}} {{CSV Files} {.csv}} {{All Files} {*}}")
        if (tclvalue(inep_file) == "") {
          cat("[AVISO] Operação de comparação com INEP cancelada.\n")
          return(NULL)
        }
        inep_file <- tclvalue(inep_file)
      }
      
      # Carregar dados do INEP
      cat(sprintf("[INFO] Lendo arquivo INEP: %s\n", inep_file))
      if (grepl(".csv$", inep_file, ignore.case=TRUE)) {
        inep_data <- data.table::fread(inep_file)
      } else {
        inep_data <- arrow::read_parquet(inep_file)
      }
      
      # Verificar se as colunas necessárias existem no arquivo INEP
      required_cols <- c("CO_POSICAO", "SG_AREA", "NU_PARAM_A", "NU_PARAM_B", "NU_PARAM_C", "CO_PROVA")
      missing_cols <- required_cols[!required_cols %in% names(inep_data)]
      
      if (length(missing_cols) > 0) {
        cat(sprintf("[ERRO] Colunas obrigatórias não encontradas no arquivo INEP: %s\n", paste(missing_cols, collapse=", ")))
        cat("[INFO] Colunas disponíveis: ", paste(names(inep_data)[1:min(10, length(names(inep_data)))], collapse=", "), "...\n")
        return(NULL)
      }
      
      # Verificar se a coluna TX_GABARITO existe (opcional, mas útil para validação)
      if ("TX_GABARITO" %in% names(inep_data)) {
        cat(sprintf("[INFO] Coluna TX_GABARITO encontrada no arquivo INEP. Será usada para validação adicional.\n"))
      } else {
        cat(sprintf("[AVISO] Coluna TX_GABARITO não encontrada no arquivo INEP. A validação de gabaritos não será possível.\n"))
      }
      
      # Extrair as colunas necessárias do CSV (incluindo TX_GABARITO se existir)
      if ("TX_GABARITO" %in% names(inep_data)) {
        inep_data <- inep_data[, c("CO_POSICAO", "SG_AREA", "NU_PARAM_A", "NU_PARAM_B", "NU_PARAM_C", "CO_PROVA", "TX_GABARITO")]
      } else {
        inep_data <- inep_data[, c("CO_POSICAO", "SG_AREA", "NU_PARAM_A", "NU_PARAM_B", "NU_PARAM_C", "CO_PROVA")]
      }
      
      # Converter colunas para números se necessário
      inep_data$NU_PARAM_A <- as.numeric(inep_data$NU_PARAM_A)
      inep_data$NU_PARAM_B <- as.numeric(inep_data$NU_PARAM_B)
      inep_data$NU_PARAM_C <- as.numeric(inep_data$NU_PARAM_C)
      
      # Padronizar tipos para comparação
      inep_data$CO_PROVA <- as.character(inep_data$CO_PROVA)
      inep_data$SG_AREA <- trimws(toupper(as.character(inep_data$SG_AREA)))
      if ("TX_GABARITO" %in% names(inep_data)) {
        inep_data$TX_GABARITO <- as.character(inep_data$TX_GABARITO)
      }
      prova_code_chr <- as.character(prova_code)
      area_code_chr <- toupper(trimws(as.character(area_code)))

      cat(sprintf("[DEBUG] Valores únicos CO_PROVA INEP: %s\n", paste(unique(inep_data$CO_PROVA), collapse=", ")))
      cat(sprintf("[DEBUG] Valores únicos SG_AREA INEP: %s\n", paste(unique(inep_data$SG_AREA), collapse=", ")))
      cat(sprintf("[DEBUG] Comparando com CO_PROVA='%s' e SG_AREA='%s'\n", prova_code_chr, area_code_chr))

      inep_filtered <- inep_data[inep_data$CO_PROVA == prova_code_chr & inep_data$SG_AREA == area_code_chr,]
       cat(sprintf("[area_code]", area_code))
       cat(sprintf("[SG_AREA]", inep_filtered$SG_AREA))
      if (nrow(inep_filtered) == 0) {
        # Tentar apenas por área se não encontrar pelo código+área
        inep_filtered <- inep_data[inep_data$SG_AREA == area_code,]
        if (nrow(inep_filtered) > 0) {
          cat(sprintf("[AVISO] Dados INEP para código %d não encontrados, mas encontrados dados para área %s\n", prova_code, area_code))
        } else {
          cat(sprintf("[AVISO] Não foram encontrados dados INEP para o código %d ou área %s\n", prova_code, area_code))
          return(NULL)
        }
      } else {
        cat(sprintf("[INFO] Encontrados %d itens nos dados INEP para código %d, área %s\n", nrow(inep_filtered), prova_code, area_code))
      }
      cat(sprintf("[INFO]", inep_filtered))
      
      # Comparar parâmetros
      all_errors <- data.frame()
      # DEBUG: Mostrar identificadores dos itens estimados e do INEP
      if (!is.null(result$item_num)) {
        cat(sprintf("[DEBUG] item_num estimados: %s\n", paste(result$item_num, collapse=", ")))
      }
      cat(sprintf("[DEBUG] CO_POSICAO INEP: %s\n", paste(inep_filtered$CO_POSICAO, collapse=", ")))
      cat(sprintf("[DEBUG] Quantidade de itens estimados: %d, itens INEP: %d\n", ifelse(!is.null(result$item_num), length(result$item_num), length(result$a)), nrow(inep_filtered)))
      
      # Verificar se TX_GABARITO existe no arquivo INEP
      has_gabarito_inep <- "TX_GABARITO" %in% names(inep_data)
      if (has_gabarito_inep) {
        cat(sprintf("[INFO] Coluna TX_GABARITO encontrada no arquivo INEP. Será usada para validação adicional.\n"))
      } else {
        cat(sprintf("[AVISO] Coluna TX_GABARITO não encontrada no arquivo INEP. Validação de gabarito não será possível.\n"))
      }
      
      error_count <- 0
      
      # Preparar tabela de resultados da comparação
      # Filtrar INEP por CO_PROVA e SG_AREA corretos (reforço)
      inep_filtered <- inep_data[inep_data$CO_PROVA == prova_code_chr & inep_data$SG_AREA == area_code_chr, ]
      cat(sprintf("[DEBUG] INEP filtrado: %d linhas para CO_PROVA=%s e SG_AREA=%s\n", nrow(inep_filtered), prova_code_chr, area_code_chr))
      # Correspondência por CO_POSICAO: estimado[i] <-> INEP[CO_POSICAO == offset + i]
      # offsets <- c("CH" = 0, "LC" = 45, "MT" = 90, "CN" = 135)
      offsets <- c("LC" = 0, "CH" = 45, "CN" = 90, "MT" = 135)
      area_code_chr <- as.character(area_code)
      offset <- offsets[area_code_chr]
      cat(sprintf("[DEBUG] offset aplicado para area %s: %d\n", area_code_chr, offset))
      comp_table <- data.frame(
        item_seq=integer(),
        CO_POSICAO=integer(),
        a_est=double(),
        a_inep=double(),
        a_error=double(),
        b_est=double(),
        b_inep=double(),
        b_error=double(),
        c_est=double(),
        c_inep=double(),
        c_error=double(),
        gabarito_est=character(),
        gabarito_inep=character(),
        gabarito_match=logical()
      )
      for (j in seq_len(nrow(inep_filtered))) {
        co_posicao <- as.integer(inep_filtered$CO_POSICAO[j])
        item_seq <- co_posicao - offset
        if (item_seq < 1 || item_seq > nrow(result)) {
          cat(sprintf("[AVISO] CO_POSICAO=%d não corresponde a nenhum item_seq válido\n", co_posicao))
          next
        }
        i <- item_seq
        inep_row <- inep_filtered[j,]
        if (nrow(inep_row) == 0) {
          cat(sprintf("[AVISO] Não encontrado CO_POSICAO=%d no INEP para item_seq=%d\n", co_posicao, i))
          next
        }
        # Diagnóstico: imprimir nomes das colunas
        if (j == 1) {
          cat(sprintf("[DEBUG] Nomes das colunas em result: %s\n", paste(names(result), collapse=", ")))
          cat(sprintf("[DEBUG] Nomes das colunas em inep_row: %s\n", paste(names(inep_row), collapse=", ")))
        }
        # Aceitar nomes alternativos para os parâmetros do INEP
        col_a <- intersect(c("PARA_A", "A", "a", "NU_PARAM_A"), names(inep_row))
        col_b <- intersect(c("PARA_B", "B", "b", "NU_PARAM_B"), names(inep_row))
        col_c <- intersect(c("PARA_C", "C", "c", "NU_PARAM_C"), names(inep_row))
        if (length(col_a) == 0 || length(col_b) == 0 || length(col_c) == 0) {
          cat(sprintf("[WARNING] Parâmetros A/B/C não encontrados para comparação na linha %d (CO_POSICAO=%d). Pulando.\n", j, co_posicao))
          next
        }
        # cat(sprintf("[DEBUG] item_seq=%d, area=%s, CO_POSICAO=%d, CO_PROVA=%s, SG_AREA=%s\n", i, area_code, co_posicao, inep_row$CO_PROVA[1], inep_row$SG_AREA[1]))
        # Comparação dos parâmetros
        a_est <- result$a[i]
        b_est <- result$b[i]
        c_est <- result$c[i]
        a_inep <- inep_row[[col_a[1]]]
        b_inep <- inep_row[[col_b[1]]]
        c_inep <- inep_row[[col_c[1]]]
        a_error <- a_est - a_inep
        b_error <- b_est - b_inep
        c_error <- c_est - c_inep
        # Extrair gabarito do INEP, se disponível
        gabarito_inep_val <- ""
        if ("TX_GABARITO" %in% names(inep_row)) {
          gabarito_inep_val <- as.character(inep_row$TX_GABARITO[1])
        }
        
        # Obter gabarito estimado
        gabarito_est_val <- result$gabarito[i]
        
        # Verificar se os gabaritos coincidem
        gabarito_match <- FALSE
        if (gabarito_inep_val != "" && gabarito_est_val != "") {
          gabarito_match <- toupper(gabarito_inep_val) == toupper(gabarito_est_val)
        }
        
        # Montar linha de comparação
        comp_row <- data.frame(
          item_seq = i,
          CO_POSICAO = co_posicao,
          a_est = a_est,
          a_inep = a_inep,
          a_error = a_error,
          b_est = b_est,
          b_inep = b_inep,
          b_error = b_error,
          c_est = c_est,
          c_inep = c_inep,
          c_error = c_error,
          gabarito_est = gabarito_est_val,
          gabarito_inep = gabarito_inep_val,
          gabarito_match = gabarito_match
        )
        comp_table <- rbind(comp_table, comp_row)
        # cat(sprintf("[DEBUG] Linha adicionada: item_seq=%d, CO_POSICAO=%d\n", i, co_posicao))
        
        # Se ainda assim não encontrar, registrar NA e pular cálculo de erro
        if (nrow(inep_row) == 0) {
          error_row <- data.frame(
            item_id = result$item_id[i],
            item_num = result$item_num[i],
            a_est = result$a[i],
            a_inep = NA,
            a_error = NA,
            b_est = result$b[i],
            b_inep = NA,
            b_error = NA,
            c_est = result$c[i],
            c_inep = NA,
            c_error = NA,
            error_levels = "sem correspondência"
          )
          all_errors <- rbind(all_errors, error_row)
        }
      }
      print(str(comp_table))
      
         # Imprimir resultados da comparação
      if (nrow(comp_table) > 0) {
        cat(sprintf("[INFO] Comparados %d itens com dados oficiais do INEP.\n", nrow(comp_table)))
        # DEBUG: Estrutura de all_errors
        cat("[DEBUG] Estrutura de all_errors:\n")
        print(str(all_errors))
        cat("[DEBUG] Primeiras linhas de all_errors:\n")
        print(head(all_errors, 10))
        cat("[DEBUG] Tipos das colunas:\n")
        print(sapply(all_errors, class))
        if (nrow(all_errors) > 0 && ncol(all_errors) > 0) {
          cat("[DEBUG] Valores NA em erros:\n")
          print(colSums(is.na(all_errors[,c('a_error','b_error','c_error')])))
          # DEBUG: Checar valores antes do pmax/order
          print(head(all_errors[,c('item_num','a_error','b_error','c_error')], 10))
          # Mostrar os 5 itens com maiores erros
          # DEBUG: Checar se as colunas são numéricas
          if (!is.numeric(all_errors$a_error) | !is.numeric(all_errors$b_error) | !is.numeric(all_errors$c_error)) {
            cat("[ERRO DEBUG] a_error, b_error ou c_error não são numéricas!\n")
          }
          worst_errors <- all_errors[order(-pmax(all_errors$a_error, all_errors$b_error, all_errors$c_error, na.rm=TRUE)),][seq_len(min(5, nrow(all_errors))),]
          cat("\n[INFO] Itens com maiores erros (top 5):\n")
        } else {
          cat("[DEBUG] all_errors vazio, nada a resumir.\n")
        }
        if (exists("worst_errors") && nrow(worst_errors) > 0) {
          for (i in seq_len(nrow(worst_errors))) {
            cat(sprintf("  Item %d: a=%.1f%%, b=%.1f%%, c=%.1f%% (nível: %s)\n", 
                     worst_errors$item_num[i], 
                     worst_errors$a_error[i],
                     worst_errors$b_error[i],
                     worst_errors$c_error[i],
                     worst_errors$error_levels[i]))
          }
        }
        
        limite_pct <- 5
        pct_a <- abs(comp_table$a_error)/pmax(abs(comp_table$a_inep),1e-8)*100
        pct_b <- abs(comp_table$b_error)/pmax(abs(comp_table$b_inep),1e-8)*100
        pct_c <- abs(comp_table$c_error)/pmax(abs(comp_table$c_inep),1e-8)*100
        mask <- (pct_a > limite_pct | pct_b > limite_pct | pct_c > limite_pct)
        mask[is.na(mask)] <- FALSE
        error_count <- sum(mask)
        max_pct_a <- max(pct_a, na.rm=TRUE)
        max_pct_b <- max(pct_b, na.rm=TRUE)
        max_pct_c <- max(pct_c, na.rm=TRUE)
        idx_a <- if (all(is.na(pct_a))) NA else which.max(pct_a)
        idx_b <- if (all(is.na(pct_b))) NA else which.max(pct_b)
        idx_c <- if (all(is.na(pct_c))) NA else which.max(pct_c)
        if (error_count > 0) {
          cat(sprintf("\n[RESULTADO] Atenção! %d/%d itens com erro acima do tolerável (>%d%%).\n", 
                   error_count, nrow(comp_table), limite_pct))
        } else {
          cat(sprintf("\n[RESULTADO] Excelente! Todos os parâmetros estão dentro da margem de erro aceitável.\n"))
          cat(sprintf("  Critério: Nenhum erro absoluto (a, b, c) excede %d%% do valor INEP correspondente para todos os itens.\n", limite_pct))
          if (error_count > 0) {
            cat(sprintf("\n[RESULTADO] Atenção! %d/%d itens com erro acima do tolerável (>%d%%).\n", 
                     error_count, nrow(comp_table), limite_pct))
          } else {
            cat(sprintf("\n[RESULTADO] Excelente! Todos os parâmetros estão dentro da margem de erro aceitável.\n"))
            cat(sprintf("  Critério: Nenhum erro absoluto (a, b, c) excede %d%% do valor INEP correspondente para todos os itens.\n", limite_pct))
          }
          cat(sprintf("  Maior percentual de erro encontrado:\n"))
          cat(sprintf("    a: %.2f%% (item_seq=%d, est=%.4f, inep=%.4f)\n", max_pct_a, comp_table$item_seq[idx_a], comp_table$a_est[idx_a], comp_table$a_inep[idx_a]))
          cat(sprintf("    b: %.2f%% (item_seq=%d, est=%.4f, inep=%.4f)\n", max_pct_b, comp_table$item_seq[idx_b], comp_table$b_est[idx_b], comp_table$b_inep[idx_b]))
          cat(sprintf("    c: %.2f%% (item_seq=%d, est=%.4f, inep=%.4f)\n", max_pct_c, comp_table$item_seq[idx_c], comp_table$c_est[idx_c], comp_table$c_inep[idx_c]))
          # Ranking dos 3 maiores erros absolutos (independente do parâmetro)
          abs_errors <- data.frame(
            item_seq = rep(comp_table$item_seq, 3),
            parametro = rep(c('a','b','c'), each=nrow(comp_table)),
            erro_abs = c(abs(comp_table$a_error), abs(comp_table$b_error), abs(comp_table$c_error)),
            erro_pct = c(pct_a, pct_b, pct_c)
          )
          top3 <- abs_errors[order(-abs_errors$erro_abs),][1:3,]
          cat("  Top 3 maiores erros absolutos (qualquer parâmetro):\n")
          for(i in 1:nrow(top3)) {
            cat(sprintf("    %dº: item_seq=%d, parâmetro=%s, erro_abs=%.4f, erro_pct=%.2f%%\n",
              i, top3$item_seq[i], top3$parametro[i], top3$erro_abs[i], top3$erro_pct[i]))
          }

        }
        # --- VISUALIZAÇÃO COMPARATIVA FINAL ---
        if (nrow(comp_table) > 0) {
          png("erro_por_item.png", width=1000, height=600)
          op <- par(no.readonly=TRUE)
          on.exit(par(op), add=TRUE)
          matplot(comp_table$item_seq, 
                  cbind(comp_table$a_error, comp_table$b_error, comp_table$c_error), 
                  type='b', pch=1:3, col=c('red','blue','darkgreen'), lty=1, lwd=2,
                  xlab='Item', ylab='Erro (estimado - INEP)',
                  main='Erro por Item: a, b, c')
          legend('topright', legend=c('a_error','b_error','c_error'), col=c('red','blue','darkgreen'), pch=1:3, lty=1, lwd=2)
          dev.off()
          
          # Salvar tabela de comparação para análise posterior
          write.csv(comp_table, "comparacao_parametros_gabaritos.csv", row.names=FALSE)
          cat(sprintf("[INFO] Tabela de comparação salva em: comparacao_parametros_gabaritos.csv\n"))
        }
        # --- FIM DA VISUALIZAÇÃO ---
        # --- MEDIDA FINAL DE ERRO POR VARIÁVEL ---
        if (nrow(comp_table) > 0) {
          # Soma dos erros absolutos
          soma_abs_a <- sum(abs(comp_table$a_error), na.rm=TRUE)
          soma_abs_b <- sum(abs(comp_table$b_error), na.rm=TRUE)
          soma_abs_c <- sum(abs(comp_table$c_error), na.rm=TRUE)
          # Média absoluta
          mean_abs_a <- mean(abs(comp_table$a_error), na.rm=TRUE)
          mean_abs_b <- mean(abs(comp_table$b_error), na.rm=TRUE)
          mean_abs_c <- mean(abs(comp_table$c_error), na.rm=TRUE)
          # RMSE
          rmse_a <- sqrt(mean((comp_table$a_error)^2, na.rm=TRUE))
          rmse_b <- sqrt(mean((comp_table$b_error)^2, na.rm=TRUE))
          rmse_c <- sqrt(mean((comp_table$c_error)^2, na.rm=TRUE))
          # Percentual médio absoluto
          pct_a <- mean(abs(comp_table$a_error)/pmax(abs(comp_table$a_inep),1e-8)*100, na.rm=TRUE)
          pct_b <- mean(abs(comp_table$b_error)/pmax(abs(comp_table$b_inep),1e-8)*100, na.rm=TRUE)
          pct_c <- mean(abs(comp_table$c_error)/pmax(abs(comp_table$c_inep),1e-8)*100, na.rm=TRUE)
          n_valid <- nrow(comp_table)
          cat(sprintf("\n[RESUMO FINAL] Métricas de erro para %d itens:\n", n_valid))
          cat(sprintf("  Soma dos erros absolutos: a=%.4f, b=%.4f, c=%.4f\n", soma_abs_a, soma_abs_b, soma_abs_c))
          cat(sprintf("  Média dos erros absolutos: a=%.4f, b=%.4f, c=%.4f\n", mean_abs_a, mean_abs_b, mean_abs_c))
          cat(sprintf("  RMSE: a=%.4f, b=%.4f, c=%.4f\n", rmse_a, rmse_b, rmse_c))
          cat(sprintf("  Percentual médio absoluto: a=%.2f%%, b=%.2f%%, c=%.2f%%\n",
            pct_a, pct_b, pct_c))
          

          
          # Relatório de correspondência de gabaritos
          if (nrow(comp_table) > 0) {
            cat(sprintf("\n[VERIFICAÇÃO DE GABARITOS]\n"))
            gabarito_matches <- sum(comp_table$gabarito_match, na.rm=TRUE)
            gabarito_total <- sum(!is.na(comp_table$gabarito_match))
            gabarito_pct <- ifelse(gabarito_total > 0, 100 * gabarito_matches / gabarito_total, 0)
            cat(sprintf("  Correspondência de gabaritos: %d/%d (%.1f%%)\n", 
                       gabarito_matches, gabarito_total, gabarito_pct))
            
            # Alertar se a correspondência for baixa
            if (gabarito_pct < 80) {
              cat(sprintf("  [ALERTA] Baixa correspondência de gabaritos! Possível problema de mapeamento de itens.\n"))
              
              # Mostrar os itens com gabaritos não correspondentes
              mismatches <- comp_table[!comp_table$gabarito_match & !is.na(comp_table$gabarito_match),]
              if (nrow(mismatches) > 0) {
                cat(sprintf("  Itens com gabaritos não correspondentes:\n"))
                for (i in 1:min(10, nrow(mismatches))) {
                  cat(sprintf("    Item %d: Estimado=%s, INEP=%s\n", 
                             mismatches$item_seq[i], 
                             mismatches$gabarito_est[i], 
                             mismatches$gabarito_inep[i]))
                }
                if (nrow(mismatches) > 10) {
                  cat(sprintf("    ... e mais %d itens\n", nrow(mismatches) - 10))
                }
              }
            } else {
              cat(sprintf("  [OK] Boa correspondência de gabaritos.\n"))
            }
          }
        }
        # --- FIM DA MEDIDA FINAL ---
        # --- TOP 5 QUESTÕES COM MAIOR ERRO ---
        cat(sprintf("\n[TOP 5 QUESTÕES COM MAIOR ERRO (A, B ou C)]\n"))
        top_all <- head(comp_table[order(-pmax(abs(comp_table$a_error), abs(comp_table$b_error), abs(comp_table$c_error), na.rm=TRUE)), ], 5)
        for (row in seq_len(nrow(top_all))) {
          i <- top_all$item_seq[row]
          errors <- c(a=abs(top_all$a_error[row]), b=abs(top_all$b_error[row]), c=abs(top_all$c_error[row]))
          max_err <- max(errors)
          cat(sprintf("  Questão %d: a=%.3f, b=%.3f, c=%.3f (max=%.3f)\n", i, top_all$a_error[row], top_all$b_error[row], top_all$c_error[row], max_err))
        }
        # Top 5 por parâmetro A
        cat(sprintf("\n[TOP 5 MAIOR ERRO EM A]\n"))
        top_a <- head(comp_table[order(-abs(comp_table$a_error)), ], 5)
        for (row in seq_len(nrow(top_a))) {
          cat(sprintf("  Questão %d: a_error=%.3f\n", top_a$item_seq[row], top_a$a_error[row]))
        }
        # Top 5 por parâmetro B
        cat(sprintf("\n[TOP 5 MAIOR ERRO EM B]\n"))
        top_b <- head(comp_table[order(-abs(comp_table$b_error)), ], 5)
        for (row in seq_len(nrow(top_b))) {
          cat(sprintf("  Questão %d: b_error=%.3f\n", top_b$item_seq[row], top_b$b_error[row]))
        }
        # Top 5 por parâmetro C
        cat(sprintf("\n[TOP 5 MAIOR ERRO EM C]\n"))
        top_c <- head(comp_table[order(-abs(comp_table$c_error)), ], 5)
        for (row in seq_len(nrow(top_c))) {
          cat(sprintf("  Questão %d: c_error=%.3f\n", top_c$item_seq[row], top_c$c_error[row]))
        }
        # --- FIM DO TOP 5 ---
        # --- RELATÓRIO DE CORRESPONDÊNCIA DE GABARITOS ---
        if (nrow(comp_table) > 0) {
          cat(sprintf("\n[VERIFICAÇÃO DE GABARITOS]\n"))
          gabarito_matches <- sum(comp_table$gabarito_match, na.rm=TRUE)
          gabarito_total <- sum(!is.na(comp_table$gabarito_match))
          gabarito_pct <- ifelse(gabarito_total > 0, 100 * gabarito_matches / gabarito_total, 0)
          cat(sprintf("  Correspondência de gabaritos: %d/%d (%.1f%%)\n", 
                     gabarito_matches, gabarito_total, gabarito_pct))
          
          # Alertar se a correspondência for baixa
          if (gabarito_pct < 80) {
            cat(sprintf("  [ALERTA] Baixa correspondência de gabaritos! Possível problema de mapeamento de itens.\n"))
            
            # Mostrar os itens com gabaritos não correspondentes
            mismatches <- comp_table[!comp_table$gabarito_match & !is.na(comp_table$gabarito_match),]
            if (nrow(mismatches) > 0) {
              cat(sprintf("  Itens com gabaritos não correspondentes:\n"))
              for (i in 1:min(10, nrow(mismatches))) {
                cat(sprintf("    Item %d: Estimado=%s, INEP=%s\n", 
                           mismatches$item_seq[i], 
                           mismatches$gabarito_est[i], 
                           mismatches$gabarito_inep[i]))
              }
              if (nrow(mismatches) > 10) {
                cat(sprintf("    ... e mais %d itens\n", nrow(mismatches) - 10))
              }
            }
          } else {
            cat(sprintf("  [OK] Boa correspondência de gabaritos.\n"))
          }
        }
        # --- FIM DO RELATÓRIO DE CORRESPONDÊNCIA DE GABARITOS ---
        return(all_errors)
      } else {
        cat("[AVISO] Não foi possível comparar nenhum item com os dados do INEP.\n")
        return(NULL)
      }
    }, error = function(e) {
      cat(sprintf("[ERRO] Falha na comparação com dados INEP: %s\n", e$message))
      return(NULL)
    })
  }
  
  # Criar componentes da interface
  frame1 <- tkframe(win)
  tkpack(tklabel(frame1, text="Arquivo de entrada:"), side="left")
  entry_input <- tkentry(frame1, textvariable=input_file, width=50)
  tkpack(entry_input, side="left", padx=5)
  tkpack(tkbutton(frame1, text="Selecionar", command=select_input_file), side="left")
  tkpack(frame1, fill="x", padx=5, pady=5)
  
  frame2 <- tkframe(win)
  tkpack(tklabel(frame2, text="Código de prova:"), side="left")
  entry_prova <- tkentry(frame2, textvariable=selected_prova, width=10)
  tkpack(entry_prova, side="left", padx=5)
  tkpack(frame2, fill="x", padx=5, pady=5)
  
  frame3 <- tkframe(win)
  tkpack(tklabel(frame3, text="Diretório de saída:"), side="left")
  entry_output <- tkentry(frame3, textvariable=output_dir, width=50)
  tkpack(entry_output, side="left", padx=5)
  tkpack(tkbutton(frame3, text="Selecionar", command=select_output_dir), side="left")
  tkpack(frame3, fill="x", padx=5, pady=5)
  
  frame4 <- tkframe(win)
  tkpack(tklabel(frame4, text="Modelo IRT:"), side="left")
  tkpack(tkradiobutton(frame4, text="1PL (Rasch)", variable=model_type, value="1PL"), side="left")
  tkpack(tkradiobutton(frame4, text="2PL", variable=model_type, value="2PL"), side="left")
  tkpack(tkradiobutton(frame4, text="3PL", variable=model_type, value="3PL"), side="left")
  tkpack(frame4, fill="x", padx=5, pady=5)
  
  # Frame para método de estimação
  method_frame <- tkframe(win)
  tkpack(tklabel(method_frame, text="Método de Estimação:"), side="left")
  tkpack(tkradiobutton(method_frame, text="EM", variable=estimation_method, value="EM"), side="left", padx=5)
  tkpack(tkradiobutton(method_frame, text="MCEM", variable=estimation_method, value="MCEM"), side="left", padx=5)
  tkpack(tkradiobutton(method_frame, text="QMCEM", variable=estimation_method, value="QMCEM"), side="left", padx=5)
  tkpack(method_frame, fill="x", padx=5, pady=5)
  
  # Frame para parâmetros de iteração
  iter_frame <- tkframe(win)
  tkpack(tklabel(iter_frame, text="Máx. Iterações:"), side="left")
  tkpack(tkentry(iter_frame, textvariable=max_iter_var, width=6), side="left", padx=5)
  tkpack(tklabel(iter_frame, text="Tolerância:"), side="left", padx=5)
  tkpack(tkentry(iter_frame, textvariable=tol_var, width=6), side="left", padx=5)
  tkpack(iter_frame, fill="x", padx=5, pady=5)
  
  # Frame para parâmetros de quadratura e theta
  quad_frame <- tkframe(win)
  tkpack(tklabel(quad_frame, text="Pontos Quadratura:"), side="left")
  tkpack(tkentry(quad_frame, textvariable=quadpts_var, width=4), side="left", padx=5)
  tkpack(tklabel(quad_frame, text="Theta Min:"), side="left", padx=5)
  tkpack(tkentry(quad_frame, textvariable=theta_min_var, width=4), side="left", padx=5)
  tkpack(tklabel(quad_frame, text="Theta Max:"), side="left", padx=5)
  tkpack(tkentry(quad_frame, textvariable=theta_max_var, width=4), side="left", padx=5)
  tkpack(quad_frame, fill="x", padx=5, pady=5)
  
  # Frame para cores e paralelismo
  cores_frame <- tkframe(win)
  tkpack(tklabel(cores_frame, text="Número de Cores CPU:"), side="left")
  tkpack(tkentry(cores_frame, textvariable=cores_var, width=4), side="left", padx=5)
  tkpack(tklabel(cores_frame, text=sprintf("(Disponíveis: %d)", parallel::detectCores())), side="left")
  tkpack(cores_frame, fill="x", padx=5, pady=5)
  
  frame5 <- tkframe(win)
  tkpack(tkbutton(frame5, text="Processar", command=process_data, width=20, fg="darkgreen"), side="left", padx=5)
  tkpack(tkbutton(frame5, text="Fechar", command=function() tkdestroy(win), width=10), side="left", padx=5)
  tkpack(frame5, fill="x", padx=5, pady=10)
  
  # Centralizar janela
  screen_width <- as.integer(tkwinfo("screenwidth", win))
  screen_height <- as.integer(tkwinfo("screenheight", win))
  win_width <- 600
  win_height <- 450
  x_pos <- as.integer((screen_width - win_width) / 2)
  y_pos <- as.integer((screen_height - win_height) / 2)
  tkwm.geometry(win, sprintf("%dx%d+%d+%d", win_width, win_height, x_pos, y_pos))
  
  # Colocar janela em foco
  tkwm.deiconify(win)
  tkfocus(win)
  
  # Manter a janela aberta e esperar até que seja fechada
  tkwait.window(win)
  
  # Retornar invisível
  return(invisible(NULL))
}

# Função para permitir acesso direto
IRT_unidim_direct <- function() {
  IRT_unidim()
}

# Executar a função principal se o script for chamado diretamente
if (!exists("dont_run_irt_unidim")) {
  cat("[INFO] Inicializando interface IRT unidimensional...\n")
  # Definir a variável para evitar inicialização automática em source()
  dont_run_irt_unidim <<- TRUE
  # Iniciar a interface
  IRT_unidim()
}
