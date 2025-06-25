require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

async function main() {
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseServiceRole = process.env.SUPABASE_SERVICE_ROLE;

    const supabase = createClient(supabaseUrl, supabaseServiceRole);

    const scrapeRoomEndpoint = process.env.SCRAPE_URL;

    const concurrencyLimit = 1;

    const startTime = Date.now();

    try {
        const { data: roomsToProcess, error: viewError } = await supabase
            .from('view_except_rooms')
            .select('room::text');

        if (viewError) {
            throw new Error(`Erro ao buscar rooms da view_except_rooms: ${viewError.message}`);
        }

        console.log(`Serão processadas ${roomsToProcess.length} novas rooms (da view_except_rooms).`);

        if (roomsToProcess.length === 0) {
            console.log("Nenhuma nova room encontrada na view_except_rooms para processar.");
            return;
        }

        let processedCount = 0;
        const totalRooms = roomsToProcess.length;

        for (let i = 0; i < totalRooms; i += concurrencyLimit) {
            const batch = roomsToProcess.slice(i, i + concurrencyLimit);
            console.log(`\nProcessando lote ${Math.floor(i / concurrencyLimit) + 1} de ${Math.ceil(totalRooms / concurrencyLimit)} (${batch.length} rooms)...`);

            const promises = batch.map(async (entry) => {
                const currentRoomId = String(entry.room);
                let payloadToUpsert = { id: currentRoomId, failed: true };

                try {
                    const scrapeResponse = await fetch(scrapeRoomEndpoint, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ room_id: currentRoomId })
                    });

                    if (!scrapeResponse.ok) {
                        const errorBody = await scrapeResponse.json();
                        throw new Error(`Erro na requisição scrape-room para ${currentRoomId}: ${JSON.stringify(errorBody)}`);
                    }

                    payloadToUpsert = { ...await scrapeResponse.json() };
                    payloadToUpsert.id = currentRoomId;
                    payloadToUpsert.failed = false;

                    console.log(`\x1b[34mProcessando room ${currentRoomId} - Payload para upsert: ${JSON.stringify(payloadToUpsert)}\x1b[0m`);

                } catch (error) {
                    console.error(`\x1b[31mErro no scrape ou preparação para room ${currentRoomId}: ${error.message}\x1b[0m`);
                } finally {
                    const { error: upsertError } = await supabase
                        .from('rooms')
                        .upsert(payloadToUpsert, { onConflict: 'id', ignoreDuplicates: false });

                    if (upsertError) {
                        console.error(`\x1b[31mErro final no upsert para room ${currentRoomId}: ${upsertError.message}\x1b[0m`);
                        return { status: 'rejected', roomId: currentRoomId, reason: upsertError.message };
                    }

                    if (payloadToUpsert.failed) {
                        console.log(`\x1b[33mRoom ${currentRoomId} marcada como falha no DB.\x1b[0m`);
                    } else {
                        console.log(`\x1b[32mRoom ${currentRoomId} processada com sucesso.\x1b[0m`);
                    }
                    processedCount++;
                    return { status: 'fulfilled', roomId: currentRoomId };
                }
            });

            await Promise.allSettled(promises);
        }
        console.log(`\nProcessamento concluído. Total de rooms processadas: ${processedCount}.`);

        const endTime = Date.now();
        const totalTimeInSeconds = ((endTime - startTime) / 1000).toFixed(2);
        console.log(`Tempo total gasto: ${totalTimeInSeconds} segundos.`);

    } catch (mainError) {
        console.error(`\x1b[31mErro fatal no processo principal: ${mainError.message}\x1b[0m`);
        process.exit(1);
    }
}

main();